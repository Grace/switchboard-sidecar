# Who this is for, and why they would pay

Written for the question a buyer actually asks, which is not "what does it do"
but "why would I not write this myself in an afternoon". The honest answer is
that the afternoon version works, and the failure modes it does not handle are
the ones that cost money quietly. Several of those are named below with the
evidence that found them, because a list of features persuades nobody and a list
of bugs somebody already hit is more useful than either.

Everything here describes the sidecar, which is what exists today. The two
deployment shapes that do not exist yet are in [`GAPS.md`](GAPS.md) item 23, and
which personas they unlock is at the end.

---

## The four personas

### 1. The platform engineer with several teams shipping AI features

**Where it hurts.** Three teams have each picked an SDK, each hardcoded a
provider key, and each written their own retry loop. Nobody can answer what the
company spends on inference, or which team spends it. When a provider has a bad
hour, three products degrade in three different ways and each team debugs it
separately.

**What changes.** One OpenAI-compatible endpoint per service, and routing decided
by a signed policy rather than by whatever each team wrote. Changing which model
serves production is a policy publish, not three deploys across three repos.
Spend is attributable by provider and model because the telemetry records it per
request rather than per team's best guess.

**What they would otherwise build.** A shared internal library, which works until
the first team pins an old version.

---

### 2. The regulated buyer who cannot use a hosted router

**Where it hurts.** Fintech, health, public sector, or anyone whose review board
asks where the prompts go. A hosted LLM router is a third party in the path of
customer data, and that is frequently the end of the conversation regardless of
the product's merits.

**What changes.** The sidecar runs inside their own task, in their own account,
on their own network. Prompts do not traverse a vendor. The provider keys stay in
the task and the process holding them binds loopback, so nothing outside the task
can reach it even by mistake — the gateway refuses to start on any other
interface.

They also get an audit story that is hard to assemble otherwise: every request
records the version of the signed routing policy that routed it, and every signed
policy is retained, so "why did this request go to that provider on that date" is
a join rather than an investigation.

**This is the persona for whom the loopback restriction is the reason to buy, not
a limitation to work around.** It is worth stating plainly in any pitch, because
the same sentence reads as a weakness to persona 3 and a requirement to this one.

---

### 3. The AI-native startup whose token spend is outgrowing its revenue

**Where it hurts.** Inference is the largest line item after payroll and nobody
can see inside it. Somebody suggests switching to a cheaper model and there is no
way to estimate the saving, or to roll it back quickly if quality drops.

**What changes.** Spend is visible per provider and per model, separated by token
kind rather than summed — a token read from cache and a token generated differ by
roughly an order of magnitude in price, and one combined number hides exactly the
thing worth acting on. Prompt caching on Anthropic is applied automatically where
a prefix repeats enough to pay for itself. Idempotent replay means a client retry
does not buy the same completion twice.

Shifting traffic to a cheaper model is a policy publish, and shifting it back is
another one.

---

### 4. The team running agents in production

**Where it hurts.** Agents make chained calls, so a single provider hiccup
multiplies. Reasoning models fail in ways that do not look like failures.

**What changes.** Failover across providers, a circuit breaker that opens on a
consecutive run *and* on a rolling window so a flapping provider is caught rather
than tolerated, a total generation deadline, and refusal to replay a request
after the provider accepted it — because replaying an accepted generation is how
you get billed twice for one answer.

Concretely, the defect this persona recognises immediately: a reasoning model can
spend its entire token budget on hidden reasoning and return an empty string,
billed in full. Measured on `gpt-5-nano` — 1024 tokens in, 1024 spent reasoning,
zero characters out, HTTP 200. An in-house wrapper returns that to the caller as
success. This treats it as a failure and routes around it, and records that the
first attempt was billed anyway.

---

## Why pay rather than build

The parts that are easy to write are easy to write. These are the parts that are
not, each found by measurement in this repository rather than reasoned about:

| The failure | Why an in-house wrapper misses it |
|---|---|
| Empty completion billed in full | HTTP 200 with a `length` finish reason looks like success |
| Replay after acceptance | The obvious retry loop double-bills whenever a response is slow rather than absent |
| A flapping provider | A consecutive-failure breaker never trips on alternate failures |
| Failover cost | The obvious code assigns usage per attempt, so the last attempt overwrites the ones that were also billed |
| Streaming that breaks mid-response | The status line is already committed, so the failure cannot be reported as a status code |

The fourth was a live bug in *this* codebase until recently, found because a
dashboard was built over the number and the number was wrong. That is the honest
argument for buying rather than building: not that the problem is conceptually
hard, but that it is made of a dozen small behaviours whose absence is invisible
until an invoice or an incident makes it visible.

---

## Which deployment shape each persona needs

| Persona | Sidecar (today) | Network-facing, single tenant (planned) | Hosted (planned) |
|---|---|---|---|
| 1 — platform engineer | works | **better**: one gateway per environment rather than per service | acceptable |
| 2 — regulated buyer | **required** | possible within their own network | **ruled out** |
| 3 — startup | works, if they run containers | works | **preferred**: nothing to operate |
| 4 — agents in production | works | works | works |

Two things this table is for. It says which customers each shape unlocks — the
hosted option is what persona 3 is waiting for, and it is the one thing persona 2
can never accept. And it makes clear that the sidecar is not a stepping stone to
be replaced: it is the only shape that serves the buyer with the strictest
requirements, so it stays.

---

## What it is not

Not a model router that picks the best model per prompt — routing is a policy
somebody signs, not a heuristic. Not a prompt framework, an eval harness or an
agent runtime. Not a hosted endpoint today; see [`GAPS.md`](GAPS.md) item 23.

Saying so is not modesty. A buyer who discovers a boundary after the trial trusts
the rest of the claims less.
