package main

type turnSummary struct {
	Role                     string                  `json:"role"`
	Turn                     string                  `json:"turn"`
	CapturedAt               string                  `json:"captured_at"`
	HookCues                 int                     `json:"hook_cues"`
	BashCalls                int                     `json:"bash_calls"`
	DelegateCalls            int                     `json:"delegate_calls"`
	CurrentReads             int                     `json:"current_reads"`
	DomainOperations         domainOperationsSummary `json:"domain_operations"`
	SubmitAttempts           int                     `json:"submit_attempts"`
	IntentSubmits            int                     `json:"intent_submits"`
	AcceptedReceipts         int                     `json:"accepted_receipts"`
	RejectedReceipts         int                     `json:"rejected_receipts"`
	SubmitDenials            int                     `json:"submit_denials"`
	SubmitInvocationFailures int                     `json:"submit_invocation_failures"`
	SubmitControlDenials     []controlDenial         `json:"submit_control_denials"`
	PostAcceptDenials        int                     `json:"post_accept_denials"`
	PrivateBindingProbes     int                     `json:"private_binding_probes"`
	AgentEnd                 bool                    `json:"agent_end"`
}

type domainOperationsSummary struct {
	Read     domainOperationSummary `json:"read"`
	Probe    domainOperationSummary `json:"probe"`
	Mutation domainOperationSummary `json:"mutation"`
}

type domainOperationSummary struct {
	Attempts  int `json:"attempts"`
	Successes int `json:"successes"`
}
