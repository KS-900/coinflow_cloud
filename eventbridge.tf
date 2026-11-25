# 1. IAM Role for EventBridge Scheduler
resource "aws_iam_role" "scheduler_role" {
  name = "coinflow-eventbridge-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# 2. Policy allowing EventBridge Scheduler to invoke your Lambda function(s)
resource "aws_iam_role_policy" "scheduler_invoke_lambda" {
  name = "coinflow-scheduler-invoke-lambda"
  role = aws_iam_role.scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = [
          aws_lambda_function.coin_markets_ingest.arn,
          aws_lambda_function.history.arn,
          aws_lambda_function.categories.arn
        ]
      }
    ]
  })
}

# 3. EventBridge Schedule for coin_markets_ingest
resource "aws_scheduler_schedule" "coin_markets_schedule" {
  name        = "coin-markets-ingest-schedule"
  description = "Triggers coin_markets_ingest Lambda on a recurring schedule"
  group_name  = "default"

  # Schedule expression in cron format (UTC time)
  schedule_expression          = "cron(0 1 * * ? *)"
  schedule_expression_timezone = "Africa/Johannesburg"

  # Flexible time window spreads API calls slightly to avoid exact-second spikes
  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 5
  }

  target {
    arn      = aws_lambda_function.coin_markets_ingest.arn
    role_arn = aws_iam_role.scheduler_role.arn


    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}

resource "aws_scheduler_schedule" "coin_history_schedule" {
  name        = "coin-history-ingest-schedule"
  description = "Triggers coin_history_ingest Lambda on a recurring schedule"
  group_name  = "default"

  # Schedule expression in cron format (UTC time)
  schedule_expression          = "cron(15 1 * * ? *)"
  schedule_expression_timezone = "Africa/Johannesburg"

  # Flexible time window spreads API calls slightly to avoid exact-second spikes
  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 5
  }

  target {
    arn      = aws_lambda_function.history.arn
    role_arn = aws_iam_role.scheduler_role.arn


    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}

resource "aws_scheduler_schedule" "coin_categories_schedule" {
  name        = "coin-categories-ingest-schedule"
  description = "Triggers coin_categories_ingest Lambda on a recurring schedule"
  group_name  = "default"

  # Schedule expression in cron format (UTC time)
  schedule_expression          = "cron(30 1 * * ? *)"
  schedule_expression_timezone = "Africa/Johannesburg"

  # Flexible time window spreads API calls slightly to avoid exact-second spikes
  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 5
  }

  target {
    arn      = aws_lambda_function.categories.arn
    role_arn = aws_iam_role.scheduler_role.arn


    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}