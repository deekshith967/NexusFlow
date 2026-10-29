-- 003_immutability_guards.sql
-- Database-level enforcement of immutability invariants that 001/002
-- deferred to application transactions (contract B.1, C.2-C.7, F.1,
-- F.9, F.13, H "Published definitions and terminal outcomes are
-- immutable").
--
-- The application transactions remain the primary mechanism: every
-- state-transition UPDATE is already conditional on the current state,
-- so the guards below never fire on the normal code paths. They exist so
-- that a defect, a manual SQL statement, or a future code path cannot
-- silently rewrite a published definition or a terminal outcome.
--
-- No state machine, transaction boundary, lock order, or event is
-- changed. Violations raise SQLSTATE 'NF001' (immutability) with the
-- offending table and key in the message.

-- ============================================================================
-- workflow_versions: valid states and B.1 lifecycle
-- ============================================================================

ALTER TABLE workflow_versions
    ADD CONSTRAINT workflow_versions_state_check
    CHECK (state IN ('DRAFT', 'PUBLISHED', 'DISCARDED', 'RETIRED'));

CREATE FUNCTION nexusflow_guard_workflow_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state <> 'DRAFT' THEN
            RAISE EXCEPTION 'workflow_versions %: % version cannot be deleted',
                OLD.workflow_version_id, OLD.state
                USING ERRCODE = 'NF001';
        END IF;
        RETURN OLD;
    END IF;

    -- Identity never changes.
    IF NEW.workflow_version_id IS DISTINCT FROM OLD.workflow_version_id
       OR NEW.workflow_id IS DISTINCT FROM OLD.workflow_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.version_number IS DISTINCT FROM OLD.version_number
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'workflow_versions %: identity columns are immutable',
            OLD.workflow_version_id
            USING ERRCODE = 'NF001';
    END IF;

    IF OLD.state = 'DRAFT' THEN
        IF NEW.state NOT IN ('DRAFT', 'PUBLISHED', 'DISCARDED') THEN
            RAISE EXCEPTION 'workflow_versions %: invalid transition DRAFT -> %',
                OLD.workflow_version_id, NEW.state
                USING ERRCODE = 'NF001';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.state = 'PUBLISHED' THEN
        -- Only PUBLISHED -> RETIRED (setting retired_at) is permitted.
        IF NEW.state = 'RETIRED'
           AND NEW.definition_hash IS NOT DISTINCT FROM OLD.definition_hash
           AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
           AND NEW.retired_at IS NOT NULL THEN
            RETURN NEW;
        END IF;

        RAISE EXCEPTION 'workflow_versions %: published versions are immutable',
            OLD.workflow_version_id
            USING ERRCODE = 'NF001';
    END IF;

    -- RETIRED and DISCARDED are terminal.
    RAISE EXCEPTION 'workflow_versions %: % versions are immutable',
        OLD.workflow_version_id, OLD.state
        USING ERRCODE = 'NF001';
END;
$$;

CREATE TRIGGER workflow_versions_immutability
    BEFORE UPDATE OR DELETE ON workflow_versions
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_workflow_version();

-- ============================================================================
-- task_definitions / task_dependencies: editable only while DRAFT
-- ============================================================================

CREATE FUNCTION nexusflow_guard_version_graph() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    version_ids UUID[];
    locked_state TEXT;
    vid UUID;
BEGIN
    IF TG_OP = 'INSERT' THEN
        version_ids := ARRAY[NEW.workflow_version_id];
    ELSIF TG_OP = 'DELETE' THEN
        version_ids := ARRAY[OLD.workflow_version_id];
    ELSE
        version_ids := ARRAY[OLD.workflow_version_id, NEW.workflow_version_id];
    END IF;

    FOREACH vid IN ARRAY version_ids LOOP
        SELECT state INTO locked_state
        FROM workflow_versions
        WHERE workflow_version_id = vid;

        IF locked_state IS DISTINCT FROM 'DRAFT' THEN
            RAISE EXCEPTION '%: definition of % version % is immutable',
                TG_TABLE_NAME, COALESCE(locked_state, 'missing'), vid
                USING ERRCODE = 'NF001';
        END IF;
    END LOOP;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER task_definitions_immutability
    BEFORE INSERT OR UPDATE OR DELETE ON task_definitions
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_version_graph();

CREATE TRIGGER task_dependencies_immutability
    BEFORE INSERT OR UPDATE OR DELETE ON task_dependencies
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_version_graph();

-- ============================================================================
-- workflow_runs: terminal states immutable; version reference fixed
-- ============================================================================

CREATE FUNCTION nexusflow_guard_workflow_run() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'workflow_runs %: terminal run (%) is immutable',
            OLD.workflow_run_id, OLD.state
            USING ERRCODE = 'NF001';
    END IF;

    -- Invariant 2: a run references exactly one immutable version.
    IF NEW.workflow_run_id IS DISTINCT FROM OLD.workflow_run_id
       OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.workflow_id IS DISTINCT FROM OLD.workflow_id
       OR NEW.workflow_version_id IS DISTINCT FROM OLD.workflow_version_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key THEN
        RAISE EXCEPTION 'workflow_runs %: identity columns are immutable',
            OLD.workflow_run_id
            USING ERRCODE = 'NF001';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER workflow_runs_immutability
    BEFORE UPDATE ON workflow_runs
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_workflow_run();

-- ============================================================================
-- task_executions: terminal states immutable; fencing token monotonic
-- ============================================================================

CREATE FUNCTION nexusflow_guard_task_execution() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- Invariant 13: a terminal task execution cannot change in Phase 1
    -- (manual retry is deferred to Phase 2).
    IF OLD.state IN ('SUCCEEDED', 'FAILED', 'SKIPPED', 'BLOCKED', 'CANCELLED') THEN
        RAISE EXCEPTION 'task_executions %: terminal task (%) is immutable',
            OLD.task_execution_id, OLD.state
            USING ERRCODE = 'NF001';
    END IF;

    -- Invariant 9: the fencing token never moves backwards.
    IF NEW.fencing_token < OLD.fencing_token THEN
        RAISE EXCEPTION 'task_executions %: fencing token cannot decrease (% -> %)',
            OLD.task_execution_id, OLD.fencing_token, NEW.fencing_token
            USING ERRCODE = 'NF001';
    END IF;

    IF NEW.task_execution_id IS DISTINCT FROM OLD.task_execution_id
       OR NEW.workflow_run_id IS DISTINCT FROM OLD.workflow_run_id
       OR NEW.task_definition_id IS DISTINCT FROM OLD.task_definition_id
       OR NEW.task_key IS DISTINCT FROM OLD.task_key THEN
        RAISE EXCEPTION 'task_executions %: identity columns are immutable',
            OLD.task_execution_id
            USING ERRCODE = 'NF001';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER task_executions_immutability
    BEFORE UPDATE ON task_executions
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_task_execution();

-- ============================================================================
-- task_attempts: terminal states immutable (B.4)
-- ============================================================================

CREATE FUNCTION nexusflow_guard_task_attempt() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED', 'ABANDONED') THEN
        RAISE EXCEPTION 'task_attempts %: terminal attempt (%) is immutable',
            OLD.task_attempt_id, OLD.state
            USING ERRCODE = 'NF001';
    END IF;

    IF NEW.task_attempt_id IS DISTINCT FROM OLD.task_attempt_id
       OR NEW.task_execution_id IS DISTINCT FROM OLD.task_execution_id
       OR NEW.attempt_number IS DISTINCT FROM OLD.attempt_number
       OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
       OR NEW.dispatch_id IS DISTINCT FROM OLD.dispatch_id THEN
        RAISE EXCEPTION 'task_attempts %: identity columns are immutable',
            OLD.task_attempt_id
            USING ERRCODE = 'NF001';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER task_attempts_immutability
    BEFORE UPDATE ON task_attempts
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_task_attempt();

-- ============================================================================
-- outbox_events: a published event is never re-opened or rewritten
-- ============================================================================

CREATE FUNCTION nexusflow_guard_outbox_event() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.published_at IS NOT NULL THEN
        RAISE EXCEPTION 'outbox_events %: published event is immutable',
            OLD.event_id
            USING ERRCODE = 'NF001';
    END IF;

    IF NEW.event_id IS DISTINCT FROM OLD.event_id
       OR NEW.event_type IS DISTINCT FROM OLD.event_type
       OR NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id
       OR NEW.payload IS DISTINCT FROM OLD.payload THEN
        RAISE EXCEPTION 'outbox_events %: event content is immutable',
            OLD.event_id
            USING ERRCODE = 'NF001';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_immutability
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION nexusflow_guard_outbox_event();
