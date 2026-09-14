import { DagValidator } from '../../src/workflow/domain/dag-validator';
import { FailurePolicy } from '../../src/workflow/domain/failure-policy';
import {
  WorkflowValidationError,
  InvalidWorkflowVersionTransitionError,
} from '../../src/workflow/domain/workflow-errors';
import { WorkflowVersion } from '../../src/workflow/domain/workflow-version';
import { WorkflowVersionState } from '../../src/workflow/domain/workflow-version-state';
import { WorkflowDefinition } from '../../src/workflow/domain/workflow-definition';

function task(
  taskKey: string,
  dependencies: readonly string[] = [],
): WorkflowDefinition['tasks'][number] {
  return {
    taskKey,
    handlerType: 'test-handler',
    handlerVersion: '1',
    configuration: {},
    inputMapping: {},
    outputMapping: {},
    retryPolicy: { maxAttempts: 1 },
    timeoutMs: 1_000,
    failurePolicy: FailurePolicy.FAIL_WORKFLOW,
    dependencies,
  };
}

function definition(
  tasks: WorkflowDefinition['tasks'],
): WorkflowDefinition {
  return {
    name: 'test-workflow',
    tasks,
  };
}

describe('DagValidator', () => {
  const validator = new DagValidator();

  it('accepts a valid DAG', () => {
    expect(() =>
      validator.validate(
        definition([
          task('fetch'),
          task('transform', ['fetch']),
          task('persist', ['transform']),
        ]),
      ),
    ).not.toThrow();
  });

  it('rejects duplicate task keys', () => {
    expect(() =>
      validator.validate(
        definition([task('fetch'), task('fetch')]),
      ),
    ).toThrow(new WorkflowValidationError('Duplicate task key: fetch'));
  });

  it('rejects self-dependencies', () => {
    expect(() =>
      validator.validate(
        definition([task('fetch', ['fetch'])]),
      ),
    ).toThrow(
      new WorkflowValidationError(
        'Task cannot depend on itself: fetch',
      ),
    );
  });

  it('rejects dependencies referencing unknown tasks', () => {
    expect(() =>
      validator.validate(
        definition([task('fetch', ['missing'])]),
      ),
    ).toThrow(
      new WorkflowValidationError(
        "Unknown dependency 'missing' referenced by task 'fetch'",
      ),
    );
  });

  it('rejects dependency cycles', () => {
    expect(() =>
      validator.validate(
        definition([
          task('a', ['c']),
          task('b', ['a']),
          task('c', ['b']),
        ]),
      ),
    ).toThrow(WorkflowValidationError);
  });

  it('rejects an empty workflow', () => {
    expect(() =>
      validator.validate(definition([])),
    ).toThrow(WorkflowValidationError);
  });

  it('rejects invalid retry configuration', () => {
    expect(() =>
      validator.validate(
        definition([
          {
            ...task('fetch'),
            retryPolicy: { maxAttempts: 0 },
          },
        ]),
      ),
    ).toThrow(WorkflowValidationError);
  });

  it('rejects invalid timeout configuration', () => {
    expect(() =>
      validator.validate(
        definition([
          {
            ...task('fetch'),
            timeoutMs: 0,
          },
        ]),
      ),
    ).toThrow(WorkflowValidationError);
  });

  it('rejects missing handler configuration', () => {
    expect(() =>
      validator.validate(
        definition([
          {
            ...task('fetch'),
            handlerType: '',
          },
        ]),
      ),
    ).toThrow(WorkflowValidationError);
  });
});

describe('WorkflowVersion', () => {
  const validDefinition = definition([
    task('fetch'),
    task('persist', ['fetch']),
  ]);

  it('starts as DRAFT', () => {
    const version = new WorkflowVersion(
      'version-1',
      'workflow-1',
      1,
      validDefinition,
    );

    expect(version.state).toBe(WorkflowVersionState.DRAFT);
  });

  it('publishes a valid draft', () => {
    const version = new WorkflowVersion(
      'version-1',
      'workflow-1',
      1,
      validDefinition,
    );

    version.publish();

    expect(version.state).toBe(WorkflowVersionState.PUBLISHED);
    expect(version.definitionHash).toMatch(/^[a-f0-9]{64}$/);
  });

  it('can discard a draft', () => {
    const version = new WorkflowVersion(
      'version-1',
      'workflow-1',
      1,
      validDefinition,
    );

    version.discard();

    expect(version.state).toBe(WorkflowVersionState.DISCARDED);
  });

  it('can retire a published version', () => {
    const version = new WorkflowVersion(
      'version-1',
      'workflow-1',
      1,
      validDefinition,
    );

    version.publish();
    version.retire();

    expect(version.state).toBe(WorkflowVersionState.RETIRED);
  });

  it('rejects invalid lifecycle transitions', () => {
    const version = new WorkflowVersion(
      'version-1',
      'workflow-1',
      1,
      validDefinition,
    );

    expect(() => version.retire()).toThrow(
      InvalidWorkflowVersionTransitionError,
    );

    version.discard();

    expect(() => version.publish()).toThrow(
      InvalidWorkflowVersionTransitionError,
    );
  });

  it('does not allow modifying a published version', () => {
    const version = new WorkflowVersion(
      'version-1',
      'workflow-1',
      1,
      validDefinition,
    );

    version.publish();

    expect(() =>
      version.updateDefinition(
        definition([task('different-task')]),
      ),
    ).toThrow();
  });

 it('does not expose mutable internal definition state', () => {
  const version = new WorkflowVersion(
    'version-1',
    'workflow-1',
    1,
    validDefinition,
  );

  version.publish();

  const exposed = version.definition;
  (exposed.tasks[0] as { taskKey: string }).taskKey = 'mutated';

  expect(version.definition.tasks[0].taskKey).toBe('fetch');
});

  it('rejects invalid definitions during publication', () => {
  const version = new WorkflowVersion(
    'version-1',
    'workflow-1',
    1,
    definition([
      task('a', ['b']),
      task('b', ['a']),
    ]),
  );

  expect(() => version.publish()).toThrow(
    WorkflowValidationError,
  );

  expect(version.state).toBe(WorkflowVersionState.DRAFT);
});
});
