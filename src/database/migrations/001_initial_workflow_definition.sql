CREATE TABLE workflows (
    workflow_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ,

    CONSTRAINT workflows_name_nonempty
        CHECK (length(trim(name)) > 0)
);

CREATE UNIQUE INDEX workflows_tenant_name_active_unique
    ON workflows (tenant_id, name)
    WHERE archived_at IS NULL;

CREATE INDEX workflows_tenant_status_idx
    ON workflows (tenant_id, status);

CREATE INDEX workflows_tenant_name_idx
    ON workflows (tenant_id, name);


CREATE TABLE workflow_versions (
    workflow_version_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    version_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    definition_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,

    CONSTRAINT workflow_versions_workflow_fk
        FOREIGN KEY (workflow_id)
        REFERENCES workflows (workflow_id),

    CONSTRAINT workflow_versions_version_number_positive
        CHECK (version_number > 0),

    CONSTRAINT workflow_versions_unique_version
        UNIQUE (workflow_id, version_number),

    CONSTRAINT workflow_versions_id_tenant_unique
        UNIQUE (workflow_version_id, tenant_id)
);

CREATE INDEX workflow_versions_workflow_state_idx
    ON workflow_versions (workflow_id, state);

CREATE INDEX workflow_versions_tenant_state_idx
    ON workflow_versions (tenant_id, state);

CREATE INDEX workflow_versions_definition_hash_idx
    ON workflow_versions (definition_hash);


CREATE TABLE task_definitions (
    task_definition_id UUID PRIMARY KEY,
    workflow_version_id UUID NOT NULL,
    task_key TEXT NOT NULL,
    handler_type TEXT NOT NULL,
    handler_version TEXT NOT NULL,
    configuration JSONB NOT NULL,
    input_mapping JSONB NOT NULL,
    output_mapping JSONB NOT NULL,
    retry_policy JSONB NOT NULL,
    timeout_ms BIGINT NOT NULL,
    failure_policy TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT task_definitions_version_fk
        FOREIGN KEY (workflow_version_id)
        REFERENCES workflow_versions (workflow_version_id),

    CONSTRAINT task_definitions_task_key_nonempty
        CHECK (length(trim(task_key)) > 0),

    CONSTRAINT task_definitions_handler_type_nonempty
        CHECK (length(trim(handler_type)) > 0),

    CONSTRAINT task_definitions_handler_version_nonempty
        CHECK (length(trim(handler_version)) > 0),

    CONSTRAINT task_definitions_timeout_positive
        CHECK (timeout_ms > 0),

    CONSTRAINT task_definitions_failure_policy_check
        CHECK (
            failure_policy IN (
                'FAIL_WORKFLOW',
                'SKIP_DOWNSTREAM'
            )
        ),

    CONSTRAINT task_definitions_unique_task_key
        UNIQUE (workflow_version_id, task_key),

    CONSTRAINT task_definitions_id_version_unique
        UNIQUE (task_definition_id, workflow_version_id)
);

CREATE INDEX task_definitions_version_handler_idx
    ON task_definitions (workflow_version_id, handler_type);


CREATE TABLE task_dependencies (
    dependency_id UUID PRIMARY KEY,
    workflow_version_id UUID NOT NULL,
    upstream_task_definition_id UUID NOT NULL,
    downstream_task_definition_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT task_dependencies_version_fk
        FOREIGN KEY (workflow_version_id)
        REFERENCES workflow_versions (workflow_version_id),

    CONSTRAINT task_dependencies_upstream_fk
        FOREIGN KEY (
            upstream_task_definition_id,
            workflow_version_id
        )
        REFERENCES task_definitions (
            task_definition_id,
            workflow_version_id
        ),

    CONSTRAINT task_dependencies_downstream_fk
        FOREIGN KEY (
            downstream_task_definition_id,
            workflow_version_id
        )
        REFERENCES task_definitions (
            task_definition_id,
            workflow_version_id
        ),

    CONSTRAINT task_dependencies_not_self
        CHECK (
            upstream_task_definition_id <> downstream_task_definition_id
        ),

    CONSTRAINT task_dependencies_unique_edge
        UNIQUE (
            workflow_version_id,
            upstream_task_definition_id,
            downstream_task_definition_id
        )
);

CREATE INDEX task_dependencies_version_upstream_idx
    ON task_dependencies (
        workflow_version_id,
        upstream_task_definition_id
    );

CREATE INDEX task_dependencies_version_downstream_idx
    ON task_dependencies (
        workflow_version_id,
        downstream_task_definition_id
    );