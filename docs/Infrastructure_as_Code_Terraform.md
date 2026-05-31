# Infrastructure as Code with Terraform

## What This Document Covers

This document explains what **Infrastructure as Code (IaC)** is, how this project's Terraform code
is organized, why you always run `terraform plan` before `terraform apply`, and how **variables**
make the same code deploy to any environment. It is written for a data engineer new to the cloud and
to Terraform. Everything maps to the files under [terraform/](../terraform/).

---

## 1. What Infrastructure as Code Is

Traditionally, you build cloud infrastructure by **clicking through the AWS Console** — create a
bucket here, a table there, wire up a queue, attach a policy. This works once, but it is:

- **Not repeatable** — to build a second identical environment you must remember and redo every
  click, perfectly, by hand.
- **Not documented** — the only record of "what exists and why" is the live console itself.
- **Not reviewable** — nobody can review a click before it happens, and mistakes are silent.
- **Not version-controlled** — you cannot diff today's setup against last month's or roll back.

**Infrastructure as Code** replaces clicking with **writing the infrastructure down as code**. You
declare the resources you want — buckets, tables, roles, queues — in text files, and a tool reads
those files and makes the cloud match them. The infrastructure becomes a software artifact: it lives
in version control, can be code-reviewed, can be rebuilt identically any number of times, and can be
destroyed and recreated on demand.

**Terraform** is the IaC tool this project uses. You describe resources in `.tf` files using
HashiCorp Configuration Language (HCL), and Terraform figures out how to create, update, or delete
real AWS resources to match what you declared.

### Declarative, not imperative

A key idea: Terraform is **declarative**. You don't write step-by-step instructions ("first make the
bucket, then enable versioning, then…"). You **declare the desired end state** ("a bucket named X
with versioning enabled"), and Terraform computes the actions needed to reach it. If the bucket
already exists and matches, Terraform does nothing. If it differs, Terraform changes only what's
needed. This is why running `apply` twice is safe — the second run sees everything already matches
and makes no changes.

### The state file

Terraform tracks what it has created in a **state file** (`terraform.tfstate`). This is its memory:
it maps each resource in your code to the real AWS resource it created. When you change the code,
Terraform compares the code, the state, and reality to decide what to do. (This is why the state file
is precious — it is the source of truth linking your code to your live infrastructure.)

---

## 2. How This Project's Terraform Is Organized

Terraform reads **all** `.tf` files in a directory and treats them as one combined configuration —
so file boundaries are purely for human organization. The canonical Terraform convention is a
**four-file core**:

| Canonical file | Purpose |
|---|---|
| `provider.tf` | Which cloud, which region, provider version |
| `variables.tf` | Declared inputs that parameterize the code |
| `main.tf` | The actual resources |
| `outputs.tf` | Values to surface after a deploy |

This project **starts from that four-file core and then splits `main.tf` by domain** so each concern
is easy to find, rather than having one enormous file. The actual layout is:

| File | What it defines |
|---|---|
| [provider.tf](../terraform/provider.tf) | AWS provider `~> 5.0`, region from a variable, and `default_tags` stamped on every resource |
| [variables.tf](../terraform/variables.tf) | All input variables (region, environment, bucket names, billing mode, alert settings…) |
| [main.tf](../terraform/main.tf) | Core data resources: S3 buckets, DynamoDB tables, Glue catalog/crawlers, IAM, log groups |
| [outputs.tf](../terraform/outputs.tf) | Useful values to copy after apply (bucket names, table names, ARNs, queue URLs) |
| [glue_jobs.tf](../terraform/glue_jobs.tf) | The five Glue jobs, the Glue workflow, and its triggers |
| [step_functions.tf](../terraform/step_functions.tf) | The Step Functions state machine and its IAM role |
| [messaging.tf](../terraform/messaging.tf) | SNS, SQS (+DLQ), EventBridge rule, EventBridge Pipe |
| [monitoring.tf](../terraform/monitoring.tf) | CloudWatch alarms, AWS Chatbot/Slack, human-readable email alerts |

So while the *convention* is "four files," this project sensibly extends it: the four-file core
(`provider`, `variables`, `main`, `outputs`) is intact, and the larger subsystems (Glue, Step
Functions, messaging, monitoring) each get their own file so a reader can open exactly the area they
care about. Terraform stitches them all back together at runtime.

### The provider — one place sets global behavior

[provider.tf](../terraform/provider.tf) pins the AWS provider and applies tags to **everything**:

```hcl
provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "music-streaming-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
```

`default_tags` means every resource is automatically tagged with the project, environment, and
"ManagedBy = terraform" — useful for cost tracking and for spotting any resource that was created by
hand instead of by code.

---

## 3. Why `terraform plan` Before `terraform apply`

Terraform has two core commands that go together:

- **`terraform plan`** — Terraform reads your code, compares it against the state file and real AWS,
  and prints **exactly what it would do**: what it will create (`+`), change (`~`), or destroy (`-`)
  — without making any changes.
- **`terraform apply`** — Terraform actually performs those actions.

**Always running `plan` first matters because infrastructure changes are high-stakes and some are
irreversible.** The plan is your chance to catch a dangerous change *before* it happens. The single
most important thing to watch for is a **destroy/replace** (`-/+`): some attribute changes force
Terraform to delete and recreate a resource. For example, a DynamoDB table's primary key cannot be
altered in place — changing it in code would make the plan show the table being **destroyed and
recreated**, which would wipe its data. Seeing that in the plan lets you stop and rethink; discovering
it after `apply` means the data is already gone.

The healthy workflow is therefore:

1. Edit the `.tf` files.
2. Run `terraform plan` and **read it carefully** — confirm the changes are what you intended, and
   that nothing unexpected is being destroyed.
3. Only then run `terraform apply`.

`plan` is the equivalent of a code review and a dry run rolled into one: it turns "I hope this does
what I think" into "I can see exactly what this does."

---

## 4. How Variables Make the Code Environment-Agnostic

The same pipeline often needs to exist in several **environments** — `dev` for experimentation,
`staging` for testing, `prod` for real traffic. You do **not** want a separate copy of the code per
environment. **Variables** let one codebase parameterize everything that differs.

This project declares its inputs in [variables.tf](../terraform/variables.tf). The key ones:

```hcl
variable "environment"   { default = "dev" }
variable "project_name"  { default = "music-streaming" }
variable "aws_region"    { default = "us-east-1" }
variable "raw_bucket_name"      { default = "music-streaming-raw" }
variable "dynamodb_billing_mode"{ default = "PAY_PER_REQUEST" }
# ... slack ids, alert email, glue db name, etc.
```

These variables flow through the code so that nothing environment-specific is hard-coded. Two
patterns make the code truly environment-agnostic:

### Pattern 1 — Names are built from variables, so environments don't collide

Resource names interpolate the variables. For example the buckets:

```hcl
bucket = "${var.raw_bucket_name}-${var.environment}"   # → music-streaming-raw-dev
```

Deploy with `environment = "dev"` and you get `…-dev` buckets; deploy the *same code* with
`environment = "prod"` and you get `…-prod` buckets. The two environments never clash, because their
names are derived from the variable. Likewise `project_name` prefixes job names, alarm names, and
ARNs throughout, so an entire parallel stack can be stood up just by changing the inputs.

### Pattern 2 — Behavior is tuned by variables, not code edits

Some variables change *behavior*, not just names:

- `dynamodb_billing_mode` lets you choose `PAY_PER_REQUEST` (on-demand) for dev and `PROVISIONED` for
  a steady prod workload — without touching the table definition.
- `slack_workspace_id` / `slack_channel_id` are empty by default; setting them *enables* the Slack
  integration (via Terraform `count`), and leaving them empty skips it. One codebase, optional
  feature.
- `alert_email` even has a **validation block** that rejects a malformed address at `plan` time —
  catching a typo before deploy.

Because every environment-specific value is a variable with a sensible default, deploying to a new
environment is a matter of supplying different variable values (via a `.tfvars` file or
`-var` flags) — **not** editing the infrastructure code. The code stays single-source; the inputs
change.

---

## 5. Why Terraform Over the Alternatives

| Alternative | Why Terraform was preferred |
|---|---|
| **AWS Console (clicking)** | Not repeatable, reviewable, or version-controlled; Terraform is all three |
| **AWS CloudFormation** | AWS-native but more verbose; Terraform's HCL is concise and multi-cloud, with a large provider ecosystem |
| **Shell scripts calling the AWS CLI** | Imperative and fragile (you handle ordering, idempotency, drift yourself); Terraform is declarative and tracks state for you |

The whole pipeline — buckets, tables, jobs, the state machine, queues, alarms, IAM roles — can be
created or torn down reproducibly, reviewed before it happens via `plan`, and re-pointed at a new
environment by changing variables. That is the payoff of Infrastructure as Code.

---

## 6. Summary

| Concept | How this project does it |
|---|---|
| **IaC** | All AWS resources declared in `.tf` files, version-controlled and reviewable |
| **Declarative model** | You declare desired state; Terraform computes the changes; re-apply is safe |
| **File structure** | Four-file core (`provider`, `variables`, `main`, `outputs`) extended with domain files (`glue_jobs`, `step_functions`, `messaging`, `monitoring`) |
| **`plan` before `apply`** | Preview every create/change/**destroy** before it happens — catches data-destroying replacements |
| **Variables** | Names built from `project_name`/`environment` so envs don't collide; behavior tuned by `dynamodb_billing_mode`, Slack/email vars — no code edits per environment |
| **Global tagging** | `default_tags` stamps Project/Environment/ManagedBy on every resource |

Terraform turns this pipeline's infrastructure into reviewable, repeatable, environment-agnostic
code — so the entire AWS stack can be understood, audited, and rebuilt from the `terraform/`
directory alone.
