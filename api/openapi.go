package api

// OpenAPIYAML contains the complete OpenAPI 3.1 specification for the Conversational Orchestrator API.
const OpenAPIYAML = `openapi: 3.1.0
info:
  title: Autonomous Agentic Orchestrator API
  version: 1.0.0
  description: >
    Production REST and Conversational Intake API for the Go Orchestrator engine.
    Cleanly separates high-level strategic reasoning (LRM Multi-Hat Engine)
    from low-level task execution over a unified concurrent event loop.
paths:
  /api/v1/chat:
    post:
      summary: Conversational Intake and Planning Assistant
      description: >
        Backed by the LRM Conversational and Planner Hats. Interactively answers
        questions regarding live system state, explains blockers, or drafts new
        hierarchical DAG plans in response to directives.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - message
              properties:
                message:
                  type: string
                  example: "Build an in-memory token bucket rate limiter in Go."
                plan_id:
                  type: string
                  description: Optional ID of the plan to focus context upon.
                history:
                  type: array
                  items:
                    type: object
                    properties:
                      role:
                        type: string
                        enum: [system, user, assistant]
                      content:
                        type: string
      responses:
        '200':
          description: Conversational response or newly generated draft plan.
          content:
            application/json:
              schema:
                type: object
                properties:
                  reply:
                    type: string
                  intent:
                    type: string
                    enum: [answer, draft_plan]
                  directive:
                    type: string
                  draft_plan:
                    $ref: '#/components/schemas/PlanSpec'
        '400':
          description: Invalid request payload.
        '500':
          description: Internal processing or LLM failure.

  /api/v1/plans:
    get:
      summary: List All Plans
      description: Returns all tracked execution plans and their statuses.
      responses:
        '200':
          description: List of execution plans.
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/PlanNode'

  /api/v1/plans/{planID}:
    get:
      summary: Get Plan Real-Time Status
      description: >
        Returns the hierarchical execution tree (Epics, Stories, Tasks) for a plan,
        including attempt counters, outputs, artifacts, and pending clarification requests.
      parameters:
        - name: planID
          in: path
          required: true
          schema:
            type: string
      responses:
        '200':
          description: Real-time plan status snapshot.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PlanNode'
        '404':
          description: Plan not found.

  /api/v1/plans/{planID}/approve:
    post:
      summary: Approve Draft Plan or Inject Plan JSON
      description: >
        Approves a draft plan or injects a raw JSON plan specification into active DAG execution.
      parameters:
        - name: planID
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: false
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/PlanSpec'
      responses:
        '200':
          description: Plan approved and scheduled for execution.
          content:
            application/json:
              schema:
                type: object
                properties:
                  success:
                    type: boolean
                  plan_id:
                    type: string
                  message:
                    type: string
        '400':
          description: Structural DAG validation failure.

  /api/v1/plans/{planID}/tasks/{taskID}/clarify:
    post:
      summary: Submit Clarification for Paused Task
      description: >
        Submits user input answering a pending clarification question for a paused task.
        Routes through the LRM Conversational Hat to amend task requirements and resume
        the blocked DAG branch.
      parameters:
        - name: planID
          in: path
          required: true
          schema:
            type: string
        - name: taskID
          in: path
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - answer
              properties:
                answer:
                  type: string
                  example: "Use a sync.Mutex instead of atomic operations for bucket locking."
      responses:
        '200':
          description: Clarification accepted and task resumed.
          content:
            application/json:
              schema:
                type: object
                properties:
                  success:
                    type: boolean
                  message:
                    type: string
                  task:
                    $ref: '#/components/schemas/TaskNode'
        '400':
          description: Task is not awaiting clarification or invalid input.
        '404':
          description: Plan or task not found.

  /openapi.yaml:
    get:
      summary: OpenAPI 3.1 Specification
      description: Returns the raw OpenAPI specification in YAML format.
      responses:
        '200':
          description: OpenAPI specification.
          content:
            application/yaml:
              schema:
                type: string

components:
  schemas:
    TaskSpec:
      type: object
      required:
        - id
        - title
        - description
        - task_type
      properties:
        id:
          type: string
        title:
          type: string
        description:
          type: string
        task_type:
          type: string
        depends_on:
          type: array
          items:
            type: string
        acceptance_criteria:
          type: array
          items:
            type: string
        params:
          type: object

    StorySpec:
      type: object
      required:
        - id
        - title
        - tasks
      properties:
        id:
          type: string
        title:
          type: string
        description:
          type: string
        tasks:
          type: array
          items:
            $ref: '#/components/schemas/TaskSpec'

    EpicSpec:
      type: object
      required:
        - id
        - title
        - stories
      properties:
        id:
          type: string
        title:
          type: string
        description:
          type: string
        stories:
          type: array
          items:
            $ref: '#/components/schemas/StorySpec'

    PlanSpec:
      type: object
      required:
        - title
        - epics
      properties:
        id:
          type: string
        title:
          type: string
        objective:
          type: string
        epics:
          type: array
          items:
            $ref: '#/components/schemas/EpicSpec'

    TaskNode:
      type: object
      properties:
        id:
          type: string
        title:
          type: string
        description:
          type: string
        task_type:
          type: string
        depends_on:
          type: array
          items:
            type: string
        acceptance_criteria:
          type: array
          items:
            type: string
        status:
          type: string
          enum: [Pending, Ready, Running, Reviewing, AwaitingClarification, Completed, Failed, Blocked]
        attempts:
          type: integer
        max_attempts:
          type: integer
        output:
          type: string
        artifacts:
          type: object
          additionalProperties:
            type: string
        feedback_history:
          type: array
          items:
            type: string
        clarifications:
          type: array
          items:
            type: string
        pending_explanation:
          type: string
        pending_question:
          type: string
        last_error:
          type: string
        last_score:
          type: number

    StoryNode:
      type: object
      properties:
        id:
          type: string
        title:
          type: string
        status:
          type: string
        tasks:
          type: array
          items:
            $ref: '#/components/schemas/TaskNode'

    EpicNode:
      type: object
      properties:
        id:
          type: string
        title:
          type: string
        status:
          type: string
        stories:
          type: array
          items:
            $ref: '#/components/schemas/StoryNode'

    PlanNode:
      type: object
      properties:
        id:
          type: string
        title:
          type: string
        objective:
          type: string
        status:
          type: string
        epics:
          type: array
          items:
            $ref: '#/components/schemas/EpicNode'
`

