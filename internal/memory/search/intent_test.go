package search

import (
	"testing"
)

func TestDetectIntent_Why(t *testing.T) {
	cases := []string{
		"why did we choose SQLite",
		"the reason we chose Go because of motivation",
		"为什么选择这个方案",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentWhy {
			t.Errorf("DetectIntent(%q) = %q, want WHY", q, got)
		}
	}
}

func TestDetectIntent_When(t *testing.T) {
	cases := []string{
		"when was the database migrated",
		"timeline of changes",
		"什么时候做的修改",
		"what happened before the release",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentWhen {
			t.Errorf("DetectIntent(%q) = %q, want WHEN", q, got)
		}
	}
}

func TestDetectIntent_Entity(t *testing.T) {
	cases := []string{
		"what is MAGMA",
		"who is responsible for the API",
		"tell me about the graph engine",
		"是什么原理",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentEntity {
			t.Errorf("DetectIntent(%q) = %q, want ENTITY", q, got)
		}
	}
}

func TestDetectIntent_General(t *testing.T) {
	cases := []string{
		"SQLite performance tuning",
		"graph traversal algorithm",
		"内存管理策略",
	}
	for _, q := range cases {
		got := DetectIntent(q)
		if got != IntentGeneral {
			t.Errorf("DetectIntent(%q) = %q, want GENERAL", q, got)
		}
	}
}

// These are the 33 native-language queries from issue #132. They measure intent
// selection, not translation quality or end-to-end retrieval accuracy.
func TestDetectIntent_MultilingualQuestions(t *testing.T) {
	tests := []struct {
		language          string
		why, when, entity string
	}{
		{"English", "Why did we choose PostgreSQL?", "When did we choose PostgreSQL?", "What is PostgreSQL?"},
		{"Mandarin Chinese", "我们为什么选择 PostgreSQL？", "我们什么时候选择了 PostgreSQL？", "PostgreSQL 是什么？"},
		{"Hindi", "हमने PostgreSQL क्यों चुना?", "हमने PostgreSQL कब चुना?", "PostgreSQL क्या है?"},
		{"Spanish", "¿Por qué elegimos PostgreSQL?", "¿Cuándo elegimos PostgreSQL?", "¿Qué es PostgreSQL?"},
		{"Modern Standard Arabic", "لماذا اخترنا PostgreSQL؟", "متى اخترنا PostgreSQL؟", "ما هو PostgreSQL؟"},
		{"French", "Pourquoi avons-nous choisi PostgreSQL ?", "Quand avons-nous choisi PostgreSQL ?", "Qu'est-ce que PostgreSQL ?"},
		{"Bengali", "আমরা PostgreSQL কেন বেছে নিয়েছি?", "আমরা কখন PostgreSQL বেছে নিয়েছি?", "PostgreSQL কী?"},
		{"Portuguese", "Por que escolhemos PostgreSQL?", "Quando escolhemos PostgreSQL?", "O que é PostgreSQL?"},
		{"Indonesian", "Mengapa kita memilih PostgreSQL?", "Kapan kita memilih PostgreSQL?", "Apa itu PostgreSQL?"},
		{"Russian", "Почему выбрали PostgreSQL?", "Когда выбрали PostgreSQL?", "Что такое PostgreSQL?"},
		{"German", "Warum haben wir PostgreSQL gewählt?", "Wann haben wir PostgreSQL gewählt?", "Was ist PostgreSQL?"},
	}
	for _, tt := range tests {
		t.Run(tt.language, func(t *testing.T) {
			for _, question := range []struct {
				query string
				want  Intent
			}{{tt.why, IntentWhy}, {tt.when, IntentWhen}, {tt.entity, IntentEntity}} {
				if got := DetectIntent(question.query); got != question.want {
					t.Errorf("DetectIntent(%q) = %q, want %q", question.query, got, question.want)
				}
			}
		})
	}
}

func TestDetectIntent_ScriptVariants(t *testing.T) {
	tests := []struct {
		query string
		want  Intent
	}{
		{"我們為什麼選擇 PostgreSQL？", IntentWhy},
		{"我們為甚麼選擇 PostgreSQL？", IntentWhy},
		{"我們什麼時候選擇 PostgreSQL？", IntentWhen},
		{"何時修改 PostgreSQL？", IntentWhen},
		{"PostgreSQL 是什麼？", IntentEntity},
		{"介紹 PostgreSQL", IntentEntity},
		{"हमने PostgreSQL क्यूँ चुना?", IntentWhy},
		{"PostgreSQL किसलिए?", IntentWhy},
		{"वह कौन हैं?", IntentEntity},
		{"¿POR QUÉ PostgreSQL?", IntentWhy},
		{"¿Por que\u0301 PostgreSQL?", IntentWhy},
		{"¿Cua\u0301ndo PostgreSQL?", IntentWhen},
		{"¿Quién es Ana?", IntentEntity},
		{"¿Que\u0301\u00a0es\tPostgreSQL?", IntentEntity},
		{"لِمَاذَا اخترنا PostgreSQL؟", IntentWhy},
		{"مَتَى اخترنا PostgreSQL؟", IntentWhen},
		{"ما هِيَ PostgreSQL؟", IntentEntity},
		{"لـماذا PostgreSQL؟", IntentWhy},
		{"Qu’est-ce que PostgreSQL ?", IntentEntity},
		{"Qu'est ce que PostgreSQL ?", IntentEntity},
		{"C’est quoi PostgreSQL ?", IntentEntity},
		{"Qui est Alice ?", IntentEntity},
		{"Qu'est-ce que 'quand' dans PostgreSQL ?", IntentEntity},
		{"PostgreSQL কবে?", IntentWhen},
		{"PostgreSQL কি? ", IntentEntity},
		{"রহিম কে?", IntentEntity},
		{"PostgreSQL por quê?", IntentWhy},
		{"PostgreSQL por que\u0302?", IntentWhy},
		{"O que e\u0301 PostgreSQL?", IntentEntity},
		{"Quem é Maria?", IntentEntity},
		{"Kenapa PostgreSQL?", IntentWhy},
		{"Siapa itu Alice?", IntentEntity},
		{"ПОЧЕМУ PostgreSQL?", IntentWhy},
		{"Зачем PostgreSQL?", IntentWhy},
		{"Кто такая Алиса?", IntentEntity},
		{"Wieso PostgreSQL?", IntentWhy},
		{"Weshalb PostgreSQL?", IntentWhy},
		{"Wer ist Alice?", IntentEntity},
	}
	for _, tt := range tests {
		if got := DetectIntent(tt.query); got != tt.want {
			t.Errorf("DetectIntent(%q) = %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestDetectIntent_MultilingualGeneralAndBoundaries(t *testing.T) {
	for _, query := range []string{
		"", "PostgreSQL index tuning", "PostgreSQL 索引调优", "PostgreSQL इंडेक्स सुधार",
		"Índices de PostgreSQL", "فهارس PostgreSQL", "Index PostgreSQL", "PostgreSQL সূচক",
		"Índices do PostgreSQL", "Indeks PostgreSQL", "Индексы PostgreSQL", "PostgreSQL Indizes",
		"क्या PostgreSQL तेज़ है?", "PostgreSQL কি ভালো?", // yes/no, not ENTITY
		"PostgreSQL কীভাবে কাজ করে?", "PostgreSQL কেমন?", // how, outside the supported forms
		"PostgreSQL いつ?", "PostgreSQL kyun?", "لم PostgreSQL", // unsupported forms/scripts
		"екогда", "когдаx", "xकब", "कब्ज", "क्योंकि", "কেননা", "xকেন", "لماذات", "متىx",
		"kapanpun", "bagaimana", "was istanbul", "ékapan", "pourquoix", "когдаé",
		"_kapan", "kapan_", "kapan2", "2kapan", "kapan\u0301", "\u0301kapan",
		"कब\u200d", "\u200cকেন", "why中文", "éwhy", "whyé", "timeé", "describé",
		`The "pourquoi" option`, "The `wann` flag", "The “когда” command", "The «क्यों» token",
		"The 'kapan' flag",
	} {
		if got := DetectIntent(query); got != IntentGeneral {
			t.Errorf("DetectIntent(%q) = %q, want GENERAL", query, got)
		}
	}
}

func TestDetectIntent_MixedAndAmbiguous(t *testing.T) {
	tests := []struct {
		query string
		want  Intent
	}{
		{"Why PostgreSQL, pourquoi ce choix?", IntentWhy},
		{"PostgreSQL कब चुना, when?", IntentWhen},
		{"Was ist PostgreSQL, 是什么?", IntentEntity},
		{"Pourquoi PostgreSQL — why, 为什么?", IntentWhy},
		{"¿Por qué no elegimos PostgreSQL?", IntentWhy},
		{"Why PostgreSQL, cuándo migramos?", IntentGeneral},
		{"Pourquoi PostgreSQL, WHEN migrated?", IntentGeneral},
		{"¿Por qué y cuándo elegimos PostgreSQL?", IntentGeneral},
		{"No por qué, sino cuándo elegimos PostgreSQL", IntentGeneral},
		{"क्यों और कब PostgreSQL?", IntentGeneral},
		{"Почему PostgreSQL, was ist PostgreSQL?", IntentGeneral},
		{"Pourquoi PostgreSQL, what is its purpose?", IntentGeneral},
		{"when when when pourquoi PostgreSQL?", IntentGeneral},
		{"Was ist the history of PostgreSQL?", IntentGeneral},
		{"What is the reason for PostgreSQL?", IntentEntity}, // legacy ENTITY tie-break
		{"why why when PostgreSQL", IntentWhy},               // legacy keyword counts
		{"为什么为什么什么时候 PostgreSQL", IntentWhy},
		{"why when PostgreSQL", IntentGeneral},
		{"timeline of PostgreSQL", IntentWhen},
		{"Please retell me about PostgreSQL", IntentEntity},
		{"Retell me about why we chose PostgreSQL", IntentEntity},
		{"étell me about PostgreSQL", IntentEntity},
		{"why why tell me about PostgreSQL", IntentWhy}, // valid compound counts once
	}
	for _, tt := range tests {
		if got := DetectIntent(tt.query); got != tt.want {
			t.Errorf("DetectIntent(%q) = %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestIntentFromString_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  Intent
	}{
		{"WHY", IntentWhy},
		{"why", IntentWhy},
		{" When ", IntentWhen},
		{"ENTITY", IntentEntity},
		{"general", IntentGeneral},
	}
	for _, tt := range tests {
		got, err := IntentFromString(tt.input)
		if err != nil {
			t.Errorf("IntentFromString(%q): unexpected error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("IntentFromString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIntentFromString_Invalid(t *testing.T) {
	_, err := IntentFromString("BOGUS")
	if err == nil {
		t.Error("IntentFromString(BOGUS): want error, got nil")
	}
}

func TestGetWeights_KnownIntents(t *testing.T) {
	for _, intent := range []Intent{IntentWhy, IntentWhen, IntentEntity, IntentGeneral} {
		w := GetWeights(intent)
		if len(w) == 0 {
			t.Errorf("GetWeights(%q): want non-empty weights", intent)
		}
		// All weights should sum to ~1.0
		var sum float64
		for _, v := range w {
			sum += v
		}
		if sum < 0.99 || sum > 1.01 {
			t.Errorf("GetWeights(%q): weights sum to %f, want ~1.0", intent, sum)
		}
	}
}

func TestGetWeights_WhyPrioritizesCausal(t *testing.T) {
	w := GetWeights(IntentWhy)
	if w["causal"] <= w["temporal"] || w["causal"] <= w["semantic"] || w["causal"] <= w["entity"] {
		t.Errorf("WHY intent should prioritize causal edges, got %v", w)
	}
}

func TestGetWeights_WhenPrioritizesTemporal(t *testing.T) {
	w := GetWeights(IntentWhen)
	if w["temporal"] <= w["causal"] || w["temporal"] <= w["semantic"] || w["temporal"] <= w["entity"] {
		t.Errorf("WHEN intent should prioritize temporal edges, got %v", w)
	}
}

func TestGetWeights_EntityPrioritizesEntity(t *testing.T) {
	w := GetWeights(IntentEntity)
	if w["entity"] <= w["temporal"] || w["entity"] <= w["causal"] {
		t.Errorf("ENTITY intent should prioritize entity edges, got %v", w)
	}
}

func TestGetWeights_UnknownFallsBackToGeneral(t *testing.T) {
	w := GetWeights(Intent("NONEXISTENT"))
	general := GetWeights(IntentGeneral)
	for k, v := range general {
		if w[k] != v {
			t.Errorf("unknown intent: weight[%s]=%f, want %f (GENERAL default)", k, w[k], v)
		}
	}
}
