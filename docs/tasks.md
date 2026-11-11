# Module Reference: `tasks` (Domain-Agnostic Execution Pipeline)

The `tasks` package implements the extensible task execution pipeline. It provides a thread-safe registry for task handlers, defines the standardized input/output contract for leaf tasks, and supplies concrete handlers for source code generation and chunked text extraction.

---

## 1. Task Execution Interface

Task execution is isolated from the orchestration state machine via the [`TaskHandler`](file:///home/jfisher/projects/GoOrchestrator/tasks/registry.go#L24) interface:

```go
type TaskHandler interface {
    Execute(ctx context.Context, input TaskInput) (TaskOutput, error)
    Description() string
}
```

### Context Passing via `TaskInput`
Workers receive strictly scoped, targeted context ([`TaskInput`](file:///home/jfisher/projects/GoOrchestrator/tasks/registry.go#L8)):
- `TaskID`, `Title`, `Description`: The task specification.
- `TaskType`: The registered handler identifier.
- `Params`: Type-specific raw JSON parameters (e.g. filename, target language, fields).
- `UpstreamOutputs`: A map of `taskID -> output` containing the primary outputs of **direct upstream dependencies only**.
- `Feedback`: The chronological history of reviewer feedback from previous rejected attempts.
- `Clarifications`: User-submitted clarification statements.
- `Attempt`: The current execution attempt number.

### Standardized `TaskOutput`
Handlers return structured artifacts ([`TaskOutput`](file:///home/jfisher/projects/GoOrchestrator/tasks/registry.go#L18)):
- `Content`: The primary output string or JSON payload presented to downstream tasks and reviewers.
- `Artifacts`: Key-value map of file paths to generated source code or file contents (`map[string]string`).
- `Metadata`: Handler-specific execution diagnostics.

---

## 2. Thread-Safe Registry: `Registry`

The [`Registry`](file:///home/jfisher/projects/GoOrchestrator/tasks/registry.go#L30) manages the mapping of task type strings (e.g. `"code_gen"`, `"text_extract"`) to handler implementations using an internal `sync.RWMutex`.

Key operations:
- `Register(taskType string, handler TaskHandler)`: Registers a handler.
- `Get(taskType string) (TaskHandler, bool)`: Retrieves a handler for execution.
- `Capabilities() map[string]string`: Returns a map of all registered task types and their descriptions, passed directly to the Planner Hat during plan drafting.

---

## 3. Concrete Handlers

### `CodeGenHandler` ([`tasks/codegen.go`](file:///home/jfisher/projects/GoOrchestrator/tasks/codegen.go))
The `CodeGenHandler` synthesizes source code, unit tests, and interface implementations using the Small Task Model (`workerClient`).

#### Input Parameters (`CodeGenParams`)
- `language`: Target programming language (e.g. `"go"`, `"python"`). Defaults to `"go"`.
- `filename`: Target file path or artifact name (e.g. `"limiter.go"`, `"limiter_test.go"`).
- `package`: Package name for modular languages (e.g. `"limiter"`).

#### Execution Behavior
1. Formats upstream interface contracts and outputs into prompt context.
2. Incorporates prior reviewer feedback (flagging them as mandatory fixes).
3. Requests a JSON payload containing file paths and complete code without stubs or placeholders.
4. Populates `TaskOutput.Artifacts` with the synthesized files.

---

### `TextExtractHandler` ([`tasks/extract.go`](file:///home/jfisher/projects/GoOrchestrator/tasks/extract.go))
The `TextExtractHandler` processes unstructured documents and large text bodies, extracting structured data points and insights via chunked analysis.

#### Input Parameters (`TextExtractParams`)
- `document`: Explicit text content to analyze. If omitted, aggregates upstream dependency outputs.
- `fields`: Array of target attributes to extract (e.g. `["goals", "constraints", "deliverables", "risks"]`).
- `chunk_size`: Character threshold for text splitting (defaults to 3,500 characters).

#### Execution Behavior
1. Splits large documents across line boundaries into sequential chunks.
2. Dispatches extraction passes to the worker model for each chunk, querying for facts, entities, keywords, and chunk summaries.
3. Aggregates insights across all chunks, deduplicating entities and keywords into a structured JSON output.

