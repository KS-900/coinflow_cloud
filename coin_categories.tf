#no variables in this script causes a duplicate data error. add in variable to avoid this error and call with var.
data "archive_file" "categories_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/build_categories"
  output_path = "${path.module}/lambda/coin_categories_ingest.zip"
}

resource "aws_iam_role" "categories" {
  name               = "coin_categories_ingest-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "categories_perms" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/coin_categories/*"]
  }
  statement {
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/coin_categories/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["${aws_s3_bucket.raw.arn}"]
  }
}

resource "aws_iam_role_policy" "categories" {
  name   = "coin_categories_ingest-policy"
  role   = aws_iam_role.categories.id
  policy = data.aws_iam_policy_document.categories_perms.json
}

resource "aws_lambda_function" "categories" {
  function_name    = "coin_categories_ingest"
  role             = aws_iam_role.categories.arn
  handler          = "coin_categories.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.categories_zip.output_path
  source_code_hash = data.archive_file.categories_zip.output_base64sha256
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