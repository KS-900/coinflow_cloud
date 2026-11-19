#no variables in this script causes a duplicate data error. add in variable to avoid this error and call with var.
data "archive_file" "history_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/build_history"
  output_path = "${path.module}/lambda/coin_history_ingest.zip"
}

resource "aws_iam_role" "history" {
  name               = "coin_history_ingest-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "history_perms" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/coin_price_history/*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_role_policy" "history" {
  name   = "coin_history_ingest-policy"
  role   = aws_iam_role.history.id
  policy = data.aws_iam_policy_document.history_perms.json
}

resource "aws_lambda_function" "history" {
  function_name    = "coin_history_ingest"
  role             = aws_iam_role.history.arn
  handler          = "coin_history.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.history_zip.output_path
  source_code_hash = data.archive_file.history_zip.output_base64sha256
  timeout          = 900
  memory_size      = 256

  environment {
    variables = {
      S3_BUCKET                   = aws_s3_bucket.raw.bucket
      COINGECKO_API_KEY           = var.api_key
      COINGECKO_CHART_DAYS        = "90"
      COINGECKO_REQUEST_DELAY_SEC = "2.5"
    }
  }
}