# Lambda Function
variable "coingecko_api_key" {
  type      = string
  sensitive = true
  default   = ""
}

# Zips everything in lambda/build into one file
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/build"
  output_path = "${path.module}/lambda/coin_markets_ingest.zip"
}

# Who is allowed to "be" this role: the Lambda service
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "coin_markets_ingest-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# What the role is allowed to do
data "aws_iam_policy_document" "lambda_perms" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/coins_markets/*"]
  }
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "coin_markets_ingest-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_perms.json
}

resource "aws_lambda_function" "coin_markets_ingest" {
  function_name    = "coin_markets_ingest"
  role             = aws_iam_role.lambda.arn
  handler          = "coin_markets.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 900
  memory_size      = 256

  environment {
    variables = {
      S3_BUCKET         = aws_s3_bucket.raw.bucket
      COINGECKO_API_KEY = var.coingecko_api_key
    }
  }
}