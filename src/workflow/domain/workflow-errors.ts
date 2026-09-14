export class WorkflowValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkflowValidationError';
  }
}

export class InvalidWorkflowVersionTransitionError extends Error {
  constructor(from: string, to: string) {
    super(`Invalid workflow version transition: ${from} -> ${to}`);
    this.name = 'InvalidWorkflowVersionTransitionError';
  }
}

export class PublishedWorkflowVersionImmutableError extends Error {
  constructor() {
    super('Published workflow versions are immutable');
    this.name = 'PublishedWorkflowVersionImmutableError';
  }
}
