import { createHash } from 'node:crypto';

import { DagValidator } from './dag-validator';
import { InvalidWorkflowVersionTransitionError } from './workflow-errors';
import {
  WorkflowDefinition,
} from './workflow-definition';
import { WorkflowVersionState } from './workflow-version-state';

export class WorkflowVersion {
  private stateValue: WorkflowVersionState;
  private definitionValue: WorkflowDefinition;
  private definitionHashValue: string | null;

  constructor(
    readonly workflowVersionId: string,
    readonly workflowId: string,
    readonly versionNumber: number,
    definition: WorkflowDefinition,
    private readonly validator: DagValidator = new DagValidator(),
  ) {
    if (!workflowVersionId.trim()) {
      throw new Error('workflowVersionId is required');
    }

    if (!workflowId.trim()) {
      throw new Error('workflowId is required');
    }

    if (!Number.isInteger(versionNumber) || versionNumber < 1) {
      throw new Error('versionNumber must be a positive integer');
    }

    this.definitionValue = cloneDefinition(definition);
    this.stateValue = WorkflowVersionState.DRAFT;
    this.definitionHashValue = null;
  }

  get state(): WorkflowVersionState {
    return this.stateValue;
  }

  get definition(): WorkflowDefinition {
    return cloneDefinition(this.definitionValue);
  }

  get definitionHash(): string | null {
    return this.definitionHashValue;
  }

  updateDefinition(definition: WorkflowDefinition): void {
    if (this.stateValue !== WorkflowVersionState.DRAFT) {
      throw new Error(
        `Only draft workflow versions can be modified; current state is ${this.stateValue}`,
      );
    }

    this.validator.validate(definition);
    this.definitionValue = cloneDefinition(definition);
  }

  publish(): void {

  if (this.stateValue !== WorkflowVersionState.DRAFT) {

    throw new InvalidWorkflowVersionTransitionError(

      this.stateValue,

      WorkflowVersionState.PUBLISHED,

    );

  }



  this.validator.validate(this.definitionValue);

  this.definitionHashValue = calculateDefinitionHash(this.definitionValue);

  this.stateValue = WorkflowVersionState.PUBLISHED;

}

  discard(): void {
    this.transition(WorkflowVersionState.DISCARDED);
  }

  retire(): void {
    this.transition(WorkflowVersionState.RETIRED);
  }

  private transition(nextState: WorkflowVersionState): void {
    const current = this.stateValue;

    const allowed =
      (current === WorkflowVersionState.DRAFT &&
        (nextState === WorkflowVersionState.PUBLISHED ||
          nextState === WorkflowVersionState.DISCARDED)) ||
      (current === WorkflowVersionState.PUBLISHED &&
        nextState === WorkflowVersionState.RETIRED);

    if (!allowed) {
      throw new InvalidWorkflowVersionTransitionError(current, nextState);
    }

    this.stateValue = nextState;
  }
}

function cloneDefinition(
  definition: WorkflowDefinition,
): WorkflowDefinition {
  return structuredClone(definition);
}

function calculateDefinitionHash(definition: WorkflowDefinition): string {
  const canonical = JSON.stringify(definition);

  return createHash('sha256').update(canonical).digest('hex');
}
