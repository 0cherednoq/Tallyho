"""Вердикт прогона: восстановление за ``T_rec``, оракул I-01…I-14, ожидания A-CH."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import select, text

from tallyho.model.states import ItemState
from tallyho.storage.tables import build_metadata
from tests.acceptance import oracle
from tests.acceptance.app.application import AUDIENCE_ID_SPAN, PAGE
from tests.acceptance.chaos.plan import WORKERS
from tests.acceptance.chaos.stand import CONNECTION_ERRORS
from tests.acceptance.oracle import InvariantReport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncConnection

    from tallyho.model.views import BatchView
    from tallyho.storage.tables import Tables
    from tests.acceptance.app.application import AcceptanceApp
    from tests.acceptance.app.domain import DomainTable
    from tests.acceptance.app.site import CatalogGenerator
    from tests.acceptance.chaos.journal import ChaosJournal
    from tests.acceptance.chaos.load import LoadDriver, Root
    from tests.acceptance.chaos.stand import Stand

__all__ = [
    "Expectation",
    "Facts",
    "OracleInput",
    "Recovery",
    "check_expectations",
    "disruption_budget",
    "run_oracle",
    "wait_recovery",
]

_PROGRESS_SQL = text("""
    SELECT (SELECT count(*) FROM th.th_item),
           (SELECT count(*) FROM th.th_item WHERE state >= 10),
           (SELECT count(*) FROM th.th_batch),
           (SELECT count(*) FROM th.th_batch WHERE state < 10),
           (SELECT count(*) FROM th.th_outbox),
           (SELECT count(*) FROM th.th_lease),
           (SELECT count(*) FROM th.th_counter_delta)
""")
_NOT_FOUND = 404
_UNAVAILABLE = 503
_ACCEPTED = 202
_SKEW_SECONDS = 300
_SKEW_TOLERANCE = 30
_SKEWED_WORKERS = 2
_LEADER_TOLERANCE = 1.0
# Все задачи эталонного приложения объявлены как `@fq.task(max_retries=3)`.
_TASK_MAX_RETRIES = 3
_VIRTUAL_ITEM = "tallyho.sub_batch"
# Незавершённые Items после восстановления: где они застряли (диагностика для I-01).
_STUCK_SQL = text("""
    SELECT count(*),
           count(*) FILTER (WHERE l.item_id IS NOT NULL),
           count(*) FILTER (WHERE o.item_id IS NOT NULL),
           count(*) FILTER (WHERE l.item_id IS NULL AND o.item_id IS NULL),
           count(*) FILTER (
               WHERE EXISTS (
                   SELECT 1 FROM flexiq.dead_letter d
                    WHERE position(i.id::text::bytea IN d.payload) > 0
               )
           ),
           count(*) FILTER (
               WHERE EXISTS (
                   SELECT 1 FROM flexiq.jobs j
                    WHERE j.status IN (0, 1) AND position(i.id::text::bytea IN j.payload) > 0
               )
           )
      FROM th.th_item i
      LEFT JOIN th.th_lease l ON l.item_id = i.id
      LEFT JOIN th.th_outbox o ON o.item_id = i.id
     WHERE i.state = 0 AND i.child_batch_id IS NULL
""")
# I-10 под отказами. Item с джобой в DLQ обязан быть error, если эта джоба у него последняя.
# Если после неё tallyho переотправил Item (lease истёк, пока брокер уже сдался) и новая
# джоба выполнилась, Item законно завершён по её итогу: запись DLQ осталась от прошлой попытки.
# Item ищется в payload джобы по тексту id из служебного `_th`.
_DEAD_ITEMS_SQL = text("""
    WITH job AS (
        SELECT id, payload, created_at FROM flexiq.jobs
        UNION ALL
        SELECT id, payload, created_at FROM flexiq.archived_jobs
    ),
    dead AS (
        SELECT DISTINCT i.id AS item_id
          FROM th.th_item i
          JOIN flexiq.dead_letter d ON position(i.id::text::bytea IN d.payload) > 0
         WHERE i.child_batch_id IS NULL
    ),
    latest AS (
        SELECT DISTINCT ON (dead.item_id) dead.item_id, job.id AS job_id
          FROM dead
          JOIN job ON position(dead.item_id::text::bytea IN job.payload) > 0
         ORDER BY dead.item_id, job.created_at DESC, job.id DESC
    )
    SELECT dead.item_id,
           EXISTS (
               SELECT 1
                 FROM latest
                 JOIN flexiq.dead_letter d ON d.original_job_id = latest.job_id
                WHERE latest.item_id = dead.item_id
           )
      FROM dead
""")
_REDELIVERY_CHAOS = frozenset({"A-CH-01", "A-CH-05", "A-CH-12"})


@dataclass(frozen=True, slots=True)
class Recovery:
    """Как стенд восстановился после остановки хаоса.

    Attributes:
        quiescent_after: Через сколько секунд после остановки хаоса все батчи стали
            терминальными, а outbox, lease и дельты опустели; ``None`` - не дождались.
        longest_stall: Самый долгий промежуток без единого изменения в учёте.
        limit: ``T_rec`` - допустимая остановка прогресса.
        waited: Сколько всего ждали до запуска оракула.
    """

    quiescent_after: float | None
    longest_stall: float
    limit: float
    waited: float

    @property
    def ok(self) -> bool:
        """Работа дошла до конца, и прогресс нигде не стоял дольше ``T_rec``."""
        return self.quiescent_after is not None and self.longest_stall <= self.limit


@dataclass(frozen=True, slots=True)
class Expectation:
    """Одно дополнительное ожидание A-CH (ACCEPTANCE §6, колонка «Дополнительно ожидаем»)."""

    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class OracleInput:
    """Всё, что оракулу нужно знать о прогоне."""

    stand: Stand
    app: AcceptanceApp
    driver: LoadDriver
    generated: CatalogGenerator
    journal: ChaosJournal


@dataclass(frozen=True, slots=True)
class Facts:
    """Наблюдения прогона, по которым проверяются ожидания A-CH."""

    restarts: Mapping[str, int]
    running: Mapping[str, bool]
    skew: Mapping[str, float]
    sweep_interval: float
    stats: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class _Probe:
    connection: AsyncConnection
    tables: Tables
    generated: CatalogGenerator


async def _progress(stand: Stand) -> tuple[int, ...] | None:
    try:
        async with stand.connect() as connection:
            row = (await connection.execute(_PROGRESS_SQL)).one()
    except CONNECTION_ERRORS:
        return None
    return tuple(int(value) for value in row)


async def wait_recovery(
    stand: Stand, driver: LoadDriver, journal: ChaosJournal, *, hard_cap: float
) -> Recovery:
    """Дождаться конца работы после остановки хаоса и измерить восстановление.

    Критерий ACCEPTANCE §6 «восстановление после последнего отказа уложилось в T_rec»
    проверяется так: после остановки хаоса учёт не должен стоять без изменений дольше
    ``T_rec``, пока остаются нетерминальные батчи или хвосты. Оставшаяся нагрузка при этом
    может дорабатывать дольше ``T_rec`` - это работа, а не зависание. Оракул запускается
    не раньше чем через ``T_rec`` после остановки хаоса (ACCEPTANCE §4).
    """
    limit = stand.settings.recovery.total_seconds()
    started = time.monotonic()
    last_change = started
    last: tuple[int, ...] | None = None
    longest = 0.0
    quiescent: float | None = None
    while time.monotonic() - started < hard_cap:
        current = await _progress(stand)
        now = time.monotonic()
        if current is not None and current != last:
            last = current
            last_change = now
        longest = max(longest, now - last_change)
        idle = current is not None and current[2] > 0 and sum(current[3:]) == 0
        if idle and driver.finished:
            quiescent = now - started
            break
        if now - last_change > limit:
            break
        await asyncio.sleep(2)
    journal.record(
        "recovery",
        quiescent_after=None if quiescent is None else round(quiescent, 3),
        longest_stall=round(longest, 3),
        t_rec=limit,
        last_progress=last,
    )
    if quiescent is not None:
        await asyncio.sleep(max(0.0, limit - (time.monotonic() - started)))
    waited = time.monotonic() - started
    return Recovery(quiescent, round(longest, 3), limit, round(waited, 3))


# ---------------------------------------------------------------------- оракул


def _flatten(view: BatchView) -> list[BatchView]:
    return [view, *(node for child in view.children.values() for node in _flatten(child))]


def _domain_table(app: AcceptanceApp, scenario: str) -> DomainTable:
    tables = {
        "S1": app.domain.invoices,
        "S2": app.domain.campaigns,
        "S3": app.domain.catalog_runs,
    }
    return tables[scenario]


async def _items(probe: _Probe, batch_id: UUID) -> list[tuple[str, int, str | None]]:
    item = probe.tables.item
    rows = (
        await probe.connection.execute(
            select(item.c.key, item.c.state, item.c.label).where(item.c.batch_id == batch_id)
        )
    ).all()
    return [(str(key), int(state), cast("str | None", label)) for key, state, label in rows]


def _s2_comparisons(
    root: Root, view: BatchView, expand: Sequence[tuple[str, int, str | None]]
) -> dict[str, tuple[int, int]]:
    """Эталон S2 с учётом страниц аудитории, которые не удалось развернуть."""
    base = (root.index + 1) * AUDIENCE_ID_SPAN
    covered: list[str] = []
    for key, state, _label in expand:
        if state != int(ItemState.OK):
            continue
        after = int(key.removeprefix("page:"))
        start = 0 if after == 0 else after - base
        covered.extend(root.addresses[start : start + PAGE])
    unique = {address.strip().casefold() for address in covered}
    send = view.children["send"].progress
    return {
        "s2.found": (send.found, len(unique)),
        "s2.duplicates": (send.duplicates, len(covered) - len(unique)),
        "s2.skipped_by_limit": (send.skipped_by_limit, 0),
    }


def _stage_404(
    prefix: str, items: Sequence[tuple[str, int, str | None]], statuses: Mapping[str, int]
) -> dict[str, tuple[int, int]]:
    ok_but_404 = sum(
        1
        for key, state, _label in items
        if statuses.get(key, _NOT_FOUND) == _NOT_FOUND and state == int(ItemState.OK)
    )
    false_404 = sum(
        1
        for key, _state, label in items
        if label == "not_found" and statuses.get(key, _NOT_FOUND) != _NOT_FOUND
    )
    return {f"{prefix}.404-finished-ok": (ok_but_404, 0), f"{prefix}.false-404": (false_404, 0)}


async def _s3_comparisons(probe: _Probe, view: BatchView) -> dict[str, tuple[int, int]]:
    """Эталон S3 с учётом страниц и карточек, разбор которых завершился ошибкой."""
    generated = probe.generated
    pages = await _items(probe, view.children["pages"].id)
    cards = await _items(probe, view.children["cards"].id)
    pdfs = await _items(probe, view.children["pdfs"].id)
    card_refs = [
        card
        for key, state, _label in pages
        if state == int(ItemState.OK)
        for card in generated.pages[int(key.removeprefix("page:")) - 1].cards
    ]
    pdf_refs = [
        pdf
        for key, state, _label in cards
        if state == int(ItemState.OK)
        for pdf in generated.card_pdfs[key]
    ]
    card_progress = view.children["cards"].progress
    pdf_progress = view.children["pdfs"].progress
    comparisons = {
        "cards.found": (card_progress.found, len(set(card_refs))),
        "cards.duplicates": (card_progress.duplicates, len(card_refs) - len(set(card_refs))),
        "pdfs.found": (pdf_progress.found, len(set(pdf_refs))),
        "pdfs.duplicates": (pdf_progress.duplicates, len(pdf_refs) - len(set(pdf_refs))),
        "skipped_by_limit": (card_progress.skipped_by_limit + pdf_progress.skipped_by_limit, 0),
    }
    comparisons.update(_stage_404("cards", cards, generated.statuses))
    comparisons.update(_stage_404("pdfs", pdfs, generated.statuses))
    return comparisons


async def _check_i07(
    probe: _Probe, roots: Mapping[UUID, tuple[Root, BatchView]], invoices: int
) -> InvariantReport:
    """I-07 под отказами: эталон считается от фактически развернувшихся источников.

    Если страница аудитории, страница каталога или карточка завершилась ошибкой
    (инъекция 1%, исчерпанные попытки, ``lease_expired``), её потомки не появляются;
    эталон учитывает ровно те источники, что завершились успешно. Без ошибок источников
    сравнение совпадает с эталонной истиной генератора.
    """
    evidence: list[str] = []
    checked = 0
    for batch_id, (root, view) in roots.items():
        if root.scenario == "S1":
            comparisons = {"s1.found": (view.progress.found, invoices)}
        elif root.scenario == "S2":
            expand = await _items(probe, view.children["expand"].id)
            comparisons = _s2_comparisons(root, view, expand)
        else:
            comparisons = await _s3_comparisons(probe, view)
        checked += len(comparisons)
        evidence.extend(
            f"{batch_id}:{name}:actual={actual}:expected={expected}"
            for name, (actual, expected) in comparisons.items()
            if actual != expected
        )
    return InvariantReport("I-07", checked, len(evidence), tuple(evidence))


def disruption_budget(journal: ChaosJournal, threads: int, workers: int) -> int:
    """Сколько задач хаос мог оборвать между внешним вызовом и commit (I-11)."""
    everyone = threads * workers
    weights = {
        "kill_worker": threads,
        "cut_network": threads,
        "term_workers": everyone,
        "kill_pg": everyone,
        "stop_pg": everyone,
        "kill_pg_on_hook": everyone,
        "stop_pg_on_flush": everyone,
    }
    return sum(weights.get(entry.event, 0) for entry in journal.entries)


async def _check_i11(
    probe: _Probe, stand: Stand, budget: int
) -> tuple[InvariantReport, dict[str, int]]:
    """I-11: внешних вызовов не больше, чем задач, ретраев и оборванных хаосом выполнений."""
    site = cast("dict[str, int]", await stand.fetch_json(f"{stand.site_url}/journal"))
    mail = cast("list[dict[str, object]]", await stand.fetch_json(f"{stand.mail_url}/journal"))
    site_retries = sum(1 for key in site if probe.generated.statuses.get(key) == _UNAVAILABLE)
    mail_retries = sum(1 for call in mail if call["status"] == _UNAVAILABLE)
    accepted = Counter(str(call["item_id"]) for call in mail if call["status"] == _ACCEPTED)
    duplicate_mails = sum(count - 1 for count in accepted.values())
    observed = sum(site.values()) + len(mail)
    report = await oracle.check_i11_external_effects(
        probe.connection,
        probe.tables,
        observed_calls=observed,
        retry_calls=site_retries + mail_retries,
        killed_after_effect=budget,
    )
    evidence = list(report.evidence)
    if duplicate_mails > budget:
        evidence.append(f"duplicate-mails={duplicate_mails}:kill-budget={budget}")
    stats = {
        "external_calls": observed,
        "external_retries": site_retries + mail_retries,
        "duplicate_mails": duplicate_mails,
        "disruption_budget": budget,
    }
    return InvariantReport("I-11", report.checked, len(evidence), tuple(evidence)), stats


async def _dead_items(connection: AsyncConnection) -> tuple[list[UUID], int]:
    """Items, чья последняя джоба в DLQ, и число Items с более ранней мёртвой джобой."""
    rows = (await connection.execute(_DEAD_ITEMS_SQL)).all()
    last_dead = [cast("UUID", item_id) for item_id, is_last in rows if is_last]
    return last_dead, len(rows) - len(last_dead)


async def _item_stats(probe: _Probe) -> dict[str, int]:
    item = probe.tables.item
    rows = (
        await probe.connection.execute(
            select(item.c.state, item.c.label, item.c.attempt).where(
                item.c.task_name != _VIRTUAL_ITEM
            )
        )
    ).all()
    labels = Counter(f"{ItemState(int(state)).name.lower()}:{label}" for state, label, _ in rows)
    stats = {f"items.{name}": count for name, count in sorted(labels.items())}
    stats["items"] = len(rows)
    stats["lease_expired_with_attempts_left"] = sum(
        1
        for _state, label, attempt in rows
        if label == "lease_expired" and int(attempt) < _TASK_MAX_RETRIES
    )
    stuck = (await probe.connection.execute(_STUCK_SQL)).one()
    names = (
        "stuck",
        "stuck.leased",
        "stuck.in_outbox",
        "stuck.dispatched",
        "stuck.job_in_dlq",
        "stuck.job_pending_or_running",
    )
    stats.update({name: int(value) for name, value in zip(names, stuck, strict=True)})
    return stats


async def run_oracle(run: OracleInput) -> tuple[list[InvariantReport], dict[str, int]]:
    """Проверить I-01…I-14 на стенде после восстановления.

    Returns:
        Отчёты по всем четырнадцати инвариантам и числа для отчёта прогона.
    """
    app, stand, roots = run.app, run.stand, run.driver.roots
    tables = build_metadata()
    views = {root.batch_id: await app.th.handle(root.batch_id).view() for root in roots}
    by_root = {root.batch_id: (root, views[root.batch_id]) for root in roots}
    domain_views = {
        root.batch_id: (_domain_table(app, root.scenario), views[root.batch_id]) for root in roots
    }
    flat = [node for view in views.values() for node in _flatten(view)]
    budget = disruption_budget(run.journal, stand.settings.threads, len(WORKERS))
    async with stand.connect() as connection:
        dead, revived = await _dead_items(connection)
        probe = _Probe(connection, tables, run.generated)
        i11, stats = await _check_i11(probe, stand, budget)
        reports = [
            await oracle.check_i01_terminal_batches(connection, tables),
            await oracle.check_i02_no_tails(connection, tables),
            await oracle.check_i03_single_finalization(connection, tables, app.domain),
            await oracle.check_i04_exact_domain_effects(connection, tables, app.domain),
            await oracle.check_i05_counter_truth(connection, tables),
            await oracle.check_i06_domain_matches_tallyho(connection, domain_views),
            await _check_i07(probe, by_root, run.driver.profile.size),
            await oracle.check_i08_monotonic_snapshots(connection, app.domain),
            await oracle.check_i09_tree_order(connection, tables, app.domain),
            await oracle.check_i10_broker_alignment(connection, tables, dead),
            i11,
            oracle.check_i12_exact_after_seal(flat),
            await oracle.check_i13_tree_consistency(connection, tables),
            oracle.check_i14_retention(()),
        ]
        stats.update(await _item_stats(probe))
    stats["batches"] = len(flat)
    stats["items_with_last_job_in_dlq"] = len(dead)
    stats["items_redispatched_after_dead_job"] = revived
    return reports, stats


# ---------------------------------------------------------------------- ожидания A-CH


def _happened(journal: ChaosJournal, name: str, *events: str) -> Expectation:
    counts = {event: len(journal.events(event)) for event in events}
    return Expectation(name, all(counts.values()), str(counts))


def _leader_expectation(journal: ChaosJournal, bound: float) -> Expectation:
    kills = journal.events("kill_leader")
    takeovers = [entry.detail.get("takeover_seconds") for entry in kills]
    measured = [float(value) for value in takeovers if isinstance(value, int | float)]
    ok = bool(kills) and len(measured) == len(kills) and max(measured) <= bound
    return Expectation(
        "второй API-процесс берёт лидерство не дольше 2 * sweep_interval",
        ok,
        f"takeover_seconds={takeovers}, bound={bound}",
    )


def _soft_stop_expectation(journal: ChaosJournal) -> Expectation:
    stops = journal.events("term_workers")
    exits = [cast("Mapping[str, object]", entry.detail["exit_seconds"]) for entry in stops]
    leases = [entry.detail["leases_left"] for entry in stops]
    ok = (
        bool(stops)
        and all(value is not None for row in exits for value in row.values())
        and all(value == 0 for value in leases)
    )
    return Expectation(
        "после SIGTERM задачи доработали или освободили lease сразу",
        ok,
        f"leases_left={leases}, exit_seconds={[dict(row) for row in exits]}",
    )


def _skew_expectation(skew: Mapping[str, float]) -> Expectation:
    shifted = sorted(round(value) for value in skew.values() if abs(value) > _SKEW_TOLERANCE)
    ok = (
        len(shifted) == _SKEWED_WORKERS
        and abs(shifted[0] + _SKEW_SECONDS) <= _SKEW_TOLERANCE
        and abs(shifted[1] - _SKEW_SECONDS) <= _SKEW_TOLERANCE
    )
    return Expectation("часы двух воркеров сдвинуты на ±5 минут", ok, f"offsets={dict(skew)}")


def _redelivery_expectation(journal: ChaosJournal) -> Expectation:
    totals: Counter[str] = Counter()
    for entry in journal.events("redeliver"):
        totals.update(cast("Mapping[str, int]", entry.detail["counts"]))
    ok = totals["requeue_job"] > 0 and totals["replay"] > 0
    return Expectation("requeue_job и replay выполнены под нагрузкой", ok, str(dict(totals)))


def _long_tx_expectation(journal: ChaosJournal) -> Expectation:
    begins = journal.events("begin_long_tx")
    ends = journal.events("end_long_tx")
    ok = (
        len(begins) == 1
        and len(ends) == 1
        and begins[0].detail["backend_xmin"] is not None
        and begins[0].detail["backend_xmin"] == ends[0].detail["backend_xmin"]
    )
    detail = {
        "backend_xmin": [entry.detail.get("backend_xmin") for entry in (*begins, *ends)],
        "held_seconds": [entry.detail.get("held_seconds") for entry in ends],
        "rate_before": [entry.detail.get("rate_before") for entry in begins],
        "rate_during": [entry.detail.get("rate_during") for entry in ends],
    }
    return Expectation("транзакция держала backend_xmin всё время", ok, str(detail))


def _lease_expectation(stats: Mapping[str, int]) -> Expectation:
    premature = stats.get("lease_expired_with_attempts_left", 0)
    return Expectation(
        "Items с истёкшим lease переотправлены, пока у задачи есть попытки",
        premature == 0,
        f"lease_expired при attempt < {_TASK_MAX_RETRIES}: {premature} Items",
    )


def check_expectations(chaos: str, journal: ChaosJournal, facts: Facts) -> list[Expectation]:
    """Проверить, что хаос действительно состоялся и дал ожидаемое поведение.

    Инварианты оракула сюда не входят: они проверяются отдельно и для всех сценариев.
    """
    # В A-CH-12 убитый воркер может стартовать при выключенном PostgreSQL: процесс сразу
    # выходит, и его поднимает политика рестарта Docker. Это работа супервизора, не падение.
    crashed = chaos != "A-CH-12" and any(facts.restarts.values())
    result = [
        Expectation(
            "процессы приложения живы и не падали сами",
            all(facts.running.values()) and not crashed,
            f"running={dict(facts.running)}, docker_restarts={dict(facts.restarts)}",
        )
    ]
    specific: Mapping[str, list[Expectation]] = {
        "A-CH-01": [_happened(journal, "воркеры убиты kill -9", "kill_worker")],
        "A-CH-02": [
            _happened(journal, "PostgreSQL остановлен обоими способами", "kill_pg", "stop_pg")
        ],
        "A-CH-03": [
            _happened(journal, "PostgreSQL убит при видимом хуке on_finalized", "kill_pg_on_hook")
        ],
        "A-CH-04": [_happened(journal, "PostgreSQL остановлен во время flush", "stop_pg_on_flush")],
        "A-CH-05": [_happened(journal, "сеть воркера разорвана", "cut_network", "heal_network")],
        "A-CH-06": [_happened(journal, "задержка сети включена", "add_latency")],
        "A-CH-07": [_leader_expectation(journal, 2 * facts.sweep_interval + _LEADER_TOLERANCE)],
        "A-CH-08": [_soft_stop_expectation(journal)],
        "A-CH-09": [_skew_expectation(facts.skew)],
        "A-CH-10": [_redelivery_expectation(journal)],
        "A-CH-11": [_long_tx_expectation(journal)],
        "A-CH-12": [
            _happened(
                journal,
                "состоялись все пять видов отказов",
                "kill_worker",
                "kill_pg",
                "cut_network",
                "add_latency",
                "redeliver",
            )
        ],
    }
    result.extend(specific[chaos])
    if chaos in _REDELIVERY_CHAOS:
        result.append(_lease_expectation(facts.stats))
    return result
