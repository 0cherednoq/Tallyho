CREATE TABLE th_batch (
	id UUID NOT NULL,
	root_id UUID NOT NULL,
	parent_id UUID,
	parent_item_id UUID,
	kind TEXT NOT NULL,
	key TEXT,
	state SMALLINT NOT NULL,
	paused_at TIMESTAMP WITH TIME ZONE,
	cancel_requested_at TIMESTAMP WITH TIME ZONE,
	cancel_reason TEXT,
	start_at TIMESTAMP WITH TIME ZONE,
	options JSONB DEFAULT '{}'::jsonb NOT NULL,
	hooks TEXT[] DEFAULT '{}'::text[] NOT NULL,
	expected_total BIGINT,
	max_in_flight INTEGER,
	max_items BIGINT,
	max_depth SMALLINT,
	on_feeder_failed SMALLINT DEFAULT 0 NOT NULL,
	deadline_at TIMESTAMP WITH TIME ZONE,
	snap_seq INTEGER DEFAULT 0 NOT NULL,
	hook_attempts SMALLINT DEFAULT 0 NOT NULL,
	hook_error TEXT,
	retention INTERVAL,
	release_required BOOLEAN DEFAULT false NOT NULL,
	released_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	finished_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id)
);

CREATE INDEX th_batch_active_updated_idx ON th_batch (updated_at) WHERE state < 10;

CREATE INDEX th_batch_deadline_idx ON th_batch (deadline_at) WHERE deadline_at IS NOT NULL AND state IN (0, 1);

CREATE INDEX th_batch_kind_idx ON th_batch (kind, id) WHERE parent_id IS NULL;

CREATE UNIQUE INDEX th_batch_kind_key_uq ON th_batch (kind, key) WHERE parent_id IS NULL AND key IS NOT NULL;

CREATE INDEX th_batch_parent_idx ON th_batch (parent_id) WHERE parent_id IS NOT NULL;

CREATE INDEX th_batch_progress_idx ON th_batch (id) WHERE state IN (0, 1) AND 'progress' = ANY (hooks);

CREATE INDEX th_batch_retention_idx ON th_batch (finished_at) WHERE id = root_id AND finished_at IS NOT NULL AND retention IS NOT NULL AND (NOT release_required OR released_at IS NOT NULL);

CREATE UNIQUE INDEX th_batch_root_key_uq ON th_batch (root_id, key) WHERE parent_id IS NOT NULL;

CREATE TABLE th_batch_attr (
	batch_id UUID NOT NULL,
	attributes JSONB DEFAULT '{}'::jsonb NOT NULL,
	memo JSONB,
	PRIMARY KEY (batch_id)
);

CREATE INDEX th_batch_attr_attributes_idx ON th_batch_attr USING gin (attributes jsonb_path_ops);

CREATE TABLE th_counter (
	batch_id UUID NOT NULL,
	slot SMALLINT NOT NULL,
	total BIGINT DEFAULT 0 NOT NULL,
	ok BIGINT DEFAULT 0 NOT NULL,
	skip BIGINT DEFAULT 0 NOT NULL,
	error BIGINT DEFAULT 0 NOT NULL,
	cancelled BIGINT DEFAULT 0 NOT NULL,
	dispatched BIGINT DEFAULT 0 NOT NULL,
	w_total BIGINT DEFAULT 0 NOT NULL,
	w_done BIGINT DEFAULT 0 NOT NULL,
	duplicates BIGINT DEFAULT 0 NOT NULL,
	skipped_by_limit BIGINT DEFAULT 0 NOT NULL,
	tree_total BIGINT DEFAULT 0 NOT NULL,
	PRIMARY KEY (batch_id, slot)
)
 WITH (fillfactor = 50, autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

CREATE TABLE th_counter_delta (
	id BIGINT GENERATED ALWAYS AS IDENTITY,
	batch_id UUID NOT NULL,
	d_total BIGINT DEFAULT 0 NOT NULL,
	d_ok BIGINT DEFAULT 0 NOT NULL,
	d_skip BIGINT DEFAULT 0 NOT NULL,
	d_error BIGINT DEFAULT 0 NOT NULL,
	d_cancelled BIGINT DEFAULT 0 NOT NULL,
	d_dispatched BIGINT DEFAULT 0 NOT NULL,
	d_w_total BIGINT DEFAULT 0 NOT NULL,
	d_w_done BIGINT DEFAULT 0 NOT NULL,
	d_duplicates BIGINT DEFAULT 0 NOT NULL,
	d_skipped_by_limit BIGINT DEFAULT 0 NOT NULL,
	d_tree_total BIGINT DEFAULT 0 NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	PRIMARY KEY (id)
)
 WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

CREATE INDEX th_counter_delta_batch_idx ON th_counter_delta (batch_id);

CREATE INDEX th_counter_delta_created_idx ON th_counter_delta (created_at, id);

CREATE TABLE th_expiry (
	item_id UUID NOT NULL,
	expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
	PRIMARY KEY (item_id)
);

CREATE INDEX th_expiry_expires_idx ON th_expiry (expires_at);

CREATE TABLE th_feed (
	feeder_id UUID NOT NULL,
	fed_id UUID NOT NULL,
	PRIMARY KEY (feeder_id, fed_id)
);

CREATE INDEX th_feed_fed_idx ON th_feed (fed_id);

CREATE TABLE th_item (
	id UUID NOT NULL,
	batch_id UUID NOT NULL,
	state SMALLINT NOT NULL,
	label TEXT,
	attempt SMALLINT DEFAULT 0 NOT NULL,
	depth SMALLINT DEFAULT 0 NOT NULL,
	task_name TEXT NOT NULL,
	payload BYTEA NOT NULL,
	options JSONB,
	key TEXT,
	child_batch_id UUID,
	weight INTEGER DEFAULT 1 NOT NULL,
	result JSONB,
	error JSONB,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	finished_at TIMESTAMP WITH TIME ZONE,
	generation INTEGER DEFAULT 0 NOT NULL,
	PRIMARY KEY (id)
)
 WITH (fillfactor = 85);

CREATE INDEX th_item_batch_idx ON th_item (batch_id, id);

CREATE UNIQUE INDEX th_item_batch_key_uq ON th_item (batch_id, key) WHERE key IS NOT NULL;

CREATE TABLE th_item_mark (
	batch_id UUID NOT NULL,
	label TEXT NOT NULL,
	item_id UUID NOT NULL,
	PRIMARY KEY (batch_id, label, item_id)
);

CREATE TABLE th_lease (
	item_id UUID NOT NULL,
	batch_id UUID NOT NULL,
	lease_until TIMESTAMP WITH TIME ZONE NOT NULL,
	worker_id TEXT NOT NULL,
	attempt SMALLINT NOT NULL,
	progress_done BIGINT,
	progress_total BIGINT,
	redelivered BOOLEAN DEFAULT false NOT NULL,
	PRIMARY KEY (item_id)
)
 WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

CREATE INDEX th_lease_batch_idx ON th_lease (batch_id);

CREATE INDEX th_lease_until_idx ON th_lease (lease_until);

CREATE TABLE th_meta (
	key TEXT NOT NULL,
	value TEXT NOT NULL,
	PRIMARY KEY (key)
);

CREATE TABLE th_metric (
	batch_id UUID NOT NULL,
	name TEXT NOT NULL,
	slot SMALLINT NOT NULL,
	value BIGINT DEFAULT 0 NOT NULL,
	PRIMARY KEY (batch_id, name, slot)
)
 WITH (fillfactor = 50, autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

CREATE TABLE th_outbox (
	id UUID NOT NULL,
	kind SMALLINT NOT NULL,
	batch_id UUID NOT NULL,
	item_id UUID,
	task_name TEXT,
	payload BYTEA,
	options JSONB,
	available_at TIMESTAMP WITH TIME ZONE NOT NULL,
	attempts SMALLINT DEFAULT 0 NOT NULL,
	PRIMARY KEY (id)
)
 WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

CREATE INDEX th_outbox_available_idx ON th_outbox (available_at);

CREATE INDEX th_outbox_batch_idx ON th_outbox (batch_id, available_at);

CREATE TABLE th_window (
	item_id UUID NOT NULL,
	batch_id UUID NOT NULL,
	PRIMARY KEY (item_id)
)
 WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

CREATE INDEX th_window_batch_idx ON th_window (batch_id);
