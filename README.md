# coinflow_cloud

You have built a pipeline that works. This project is about proving you can build one that survives contact with a cloud bill, an IAM policy, and someone else's review.

We are not starting a new project. CoinFlow is the same product it was: CoinGecko in, a dashboard out. What changes is everything underneath it. By the end you will not have learned "AWS." You will have learned why the batch job you already wrote costs forty times more to query if you partition it wrong, and that is a more useful thing to know.

What you are proving

Three things, in this order.

That you can lay data out in object storage so it is cheap to query. That you can run transformation against it without a database server you manage. That you can explain both decisions to someone who is paying for them.

Streaming comes last, if it comes at all. Most junior data engineering roles are batch. The ones that are not will still ask you what a partition key is.

Ground rules

Everything is defined in code. If you clicked it in the console, it does not exist. You will hate this in week one and thank me in week three when you tear the whole stack down and rebuild it in four minutes.

Region is af-south-1. Not us-east-1. Latency is irrelevant here but data residency is a conversation you will have in every South African interview, and you should have already had it with yourself.

Ticket discipline stays exactly as it was. WIP limit of two. Titled, sized, acceptance criteria first. The Git history explains why, not what. This part you already do well. Do not drop it because the tooling got shinier.

Cost is a first class concern from day one, not a cleanup task at the end. Every ticket that provisions something answers: what does this cost if I forget about it for a week?

**Phase 1: Land it properly**

Two to three weeks. This is the phase that matters most and the one you will be tempted to rush.

The goal is raw CoinGecko data sitting in S3 as partitioned Parquet, catalogued, queryable from Athena, with nothing running on a schedule you did not define in code.

Ingestion. Your existing Python moves into a Lambda. Same requests logic, same error handling, same environment variable discipline. It writes raw JSON to s3://<bucket>/raw/coins_markets/dt=YYYY-MM-DD/. Untouched. Whatever CoinGecko sent you is what lands. If the API changes shape next month you want the evidence.

Scheduling. EventBridge rule, daily. Not cron on a machine. Not you running it by hand.

Conversion. A second Lambda reads the day's raw prefix and writes Parquet to s3://<bucket>/curated/coins_markets/dt=YYYY-MM-DD/. Snappy compression. Explicit schema, not inferred. This is where you will first meet the difference between a file format and a table format, and it is worth sitting with.

Catalogue. Glue Crawler over the curated prefix. Then query it from Athena. Then look at the bytes scanned.

**Acceptance criteria for the phase**

Terraform or CDK in the repo. destroy then apply reproduces the whole stack from nothing.
No IAM policy contains a wildcard on both action and resource. Every role does one job.
Raw and curated are separate prefixes with separate lifecycle rules. Raw goes to Glacier after 90 days.
Athena returns yesterday's top ten coins by market cap.
A README section showing bytes scanned for the same query, partitioned and unpartitioned, with the cost difference in rand.

That last one is not busywork. It is the paragraph you will use in an interview.

**Phase 2: Transform where the data lives**

Two to three weeks.

Your dbt models come across nearly unchanged. That is the point. dbt is the portable skill, the warehouse underneath it is an implementation detail, and understanding that is worth more than either tool individually.

Engine. dbt-athena first. If you hit its limits, and you might around incremental models, move to Redshift Serverless and note in the repo what forced the move. Documenting the constraint you hit is more impressive than picking correctly first time.

Models. Staging, intermediate, marts, same as before. dim_coins, fct_daily_coin_performance, fct_market_summary all survive. Your existing tests come with them.

Quality. dbt tests for the deterministic checks you already have: nulls, uniqueness, positive market cap. Add Great Expectations if you want a second layer with better failure reporting. Do not put an LLM in this path. We will come back to that.

Orchestration. Step Functions. Not MWAA. MWAA costs roughly USD 350 a month whether or not it runs anything, and it is the single fastest way to burn through a learning budget. Step Functions is per transition and effectively free at your volume. You already understand DAGs from Airflow, so the concept transfers. The syntax is worse. Learn it anyway, because plenty of shops run it.

**Acceptance criteria for the phase**

Full pipeline runs unattended: ingest, convert, catalogue, transform, test.
A failing dbt test stops the state machine and alerts. It does not pass silently.
dbt docs generated and committed.
Monthly cost estimate for the whole stack at current volume, written down, with the three largest line items named.
Phase 3: Streaming, and where AI actually belongs

Optional. Only after Phase 2 is genuinely finished, and finished means the acceptance criteria are met, not that you are bored of it.

Kinesis Data Streams into Firehose, Firehose buffering to Parquet in S3 on the same curated layout you already built. The reason this comes last is that streaming only makes sense once you understand the table it lands in. Do it first and you learn a vocabulary without a mental model.

Cost warning, and this one is not negotiable. A Kinesis stream left running is the most likely way you cost me real money on this project. Provisioned shards bill per hour regardless of throughput. Every Phase 3 session ends with terraform destroy on the streaming stack. Not "I will do it tomorrow."

On the AI quality checks. You suggested this and the instinct is good, but the placement needs correcting.

An LLM must not decide whether a row is valid. Validity is deterministic. A null is a null, a negative market cap is impossible, a duplicate primary key is a bug. Deterministic tests catch those every time, cost nothing, and never hallucinate. Put a Bedrock call in that path and a good interviewer will ask why, and there is no answer that survives the follow up question.

Where it does belong: after the tests pass, take the day's anomalies that your deterministic rules already flagged and have Bedrock write a plain English summary for the dashboard. "Three coins moved more than 40 percent today, all of them below USD 50m market cap, consistent with low liquidity rather than a market event." That is genuinely useful, it is cheap, and it is a good talking point because it shows you know the difference between describing and gating.

LLMs describe. Tests gate. Do not mix them up.

**What good looks like at the end**

You can stand the stack up from an empty account in under ten minutes with one command.

You can name what every IAM role is allowed to do and why it needs to be.

You can explain, in rand, what partitioning saved you.

You can say what you would do differently if this had to handle a hundred times the volume, and the answer is specific rather than "scale it up."

The README a reviewer reads is the one you would want to read if you were reviewing someone else.

**What I need from you**

Ticket the phases before you write any code. Sized, with acceptance criteria, WIP two.

Ask early when you are stuck. There is no credit for a week lost to an IAM policy. There is credit for asking on day one and writing down what the actual problem was.

Tell me before you provision anything with an hourly charge. Kinesis, Redshift, MWAA, NAT Gateways. Those four are the ones that hurt.

**One-time setup**

Install the AWS CLI v2 if you do not have it. Check with aws --version, it must say aws-cli/2.x. On Mac, brew install awscli.

Activate your account. You will get an email inviting you to the AWS access portal. Set your password and enrol an MFA app (Google Authenticator, Authy, whatever you use). You cannot log in without MFA, that is deliberate.

**Configure your CLI profile. Run this once:**

aws configure sso

Answer the prompts:

SSO session name: mojima
SSO start URL: https://d-c6677f0617.awsapps.com/start
SSO region: af-south-1
SSO registration scopes: [press enter for the default]

A browser opens, you sign in, you approve the request. Back in the terminal it shows you the account and role. Pick:

Account: Sandbox (939898395367)
Role: SandboxEngineer

Then finish the prompts:

CLI default client Region: af-south-1
CLI default output format: json
CLI profile name: sandbox

That writes a profile called sandbox into ~/.aws/config. You only do this once.

Every working session

Your credentials expire (4 hours). When they do, log in again:

aws sso login --profile sandbox

Then either prefix every command with the profile:

aws s3 ls --profile sandbox

or set it for the whole terminal session so you can forget about it:

export AWS_PROFILE=sandbox

Put that export in your shell profile if you like, since this is the only account you use here.

Check it worked
aws sts get-caller-identity

You want to see account 939898395367 and an assumed-role ARN ending in SandboxEngineer. If you see that, you are in.

Terraform picks this up for free

The AWS provider reads the same profile, so you do not configure credentials in Terraform at all. Your provider block is just:

hcl
provider "aws" {
  region  = "af-south-1"
  profile = "sandbox"
}

Run aws sso login --profile sandbox before a terraform apply if your session has expired. If Terraform complains about expired or missing credentials, that is almost always the fix.

Things that will trip you up

Everything lives in af-south-1 (Cape Town). If you create something in another region it will fail, the account is locked to Cape Town on purpose. If a command hangs or denies for no obvious reason, check your region first.

"Token has expired" or "sso session expired" just means log in again. It is not a broken setup.
