package llm

// Request tags identify the logical caller of an LLM completion. They are used
// for logging and by the deterministic mock backend; they never reach the wire.
const (
	TagConversational    = "lrm:conversational"
	TagClarification     = "lrm:conversational:clarification"
	TagPlanner           = "lrm:planner"
	TagProjectManager    = "lrm:project_manager"
	TagReviewer          = "lrm:reviewer"
	TagWorkerCodeGen     = "worker:code_gen"
	TagWorkerTextExtract = "worker:text_extract"
	TagWorkerCompact     = "worker:compact"
)

// Metadata keys attached to CompletionRequest.Metadata.
const (
	MetaMessage          = "message"
	MetaStateSummary     = "state_summary"
	MetaDirective        = "directive"
	MetaTaskTypes        = "task_types"
	MetaTaskID           = "task_id"
	MetaTitle            = "title"
	MetaDescription      = "description"
	MetaTaskType         = "task_type"
	MetaReason           = "reason"
	MetaCanSplit         = "can_split"
	MetaFeedback         = "feedback"
	MetaOutput           = "output"
	MetaAttempt          = "attempt"
	MetaHasFeedback      = "has_feedback"
	MetaHasClarification = "has_clarification"
	MetaAnswer           = "answer"
	MetaQuestion         = "question"
	MetaChunk            = "chunk"
	MetaChunkIndex       = "chunk_index"
	MetaFields           = "fields"
	MetaText             = "text"
	MetaBudget           = "budget"
	MetaLanguage         = "language"
	MetaFileName         = "filename"
	MetaPackage          = "package"
	MetaUpstream         = "upstream"
)

