from airflow import DAG
from airflow.operators.python import PythonOperator
from airflowfusion.operator import FusedPythonOperator
from airflowfusion.fuse import create_optimized_dag_integer_programming
from airflowfusion.backend_registry import read, write
import base64
import re
import boto3
import json
import os
import uuid
import zipfile
import io
from markdown import markdown
from botocore.exceptions import ClientError

s3_bucket = os.environ.get("S3_BUCKET")

def generate_blog_with_bedrock():
    input_text = "Website about dogs."
    s3 = boto3.client('s3')
    bedrock = boto3.client('bedrock-runtime', region_name='us-west-2')

    # Format the prompt
    prompt = (
        f"{input_text} Include a title, an introduction section, sub sections "
        "and a conclusion section, and output in Markdown. Respond in <response> XML tag."
    )
    body = {
        "prompt": f"Human: \n\nHuman: {prompt}\n\n\nAssistant:",
        "max_tokens_to_sample": 600,
        "temperature": 1,
        "top_k": 250,
        "top_p": 0.999,
        "stop_sequences": ["\n\nHuman:"],
        "anthropic_version": "bedrock-2023-05-31"
    }

    # Send prompt to Claude via Bedrock
    response = bedrock.invoke_model(
        modelId="anthropic.claude-v2",
        contentType="application/json",
        accept="*/*",
        body=json.dumps(body)
    )

    # Parse output
    body_bytes = response['body'].read()
    completion = json.loads(body_bytes.decode('utf-8'))['completion']

    # Extract markdown content from Claude's response
    try:
        md_text = completion.split('<response>\n\n')[1].split('\n\n</response>')[0]
    except IndexError:
        raise ValueError("Claude response did not contain valid <response> tags.")

    # Save to S3
    s3_key = 'GEN-CONTENT/blog-text.md'
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=md_text.encode("utf-8"))

    # Extract intro text
    if '## Introduction' in md_text:
        get_intro = md_text.split('## Introduction\n\n')[1].split('\n\n##')[0]
    else:
        get_intro = md_text.split('\n\n')[1]

    write('xcom', 'intro', get_intro)
    write('xcom', 'textLocation', s3_key)

def generate_image_from_intro():
    intro = read('xcom', 'intro')
    text_location = read('xcom', 'textLocation')

    if not s3_bucket:
        raise ValueError("S3_BUCKET must be provided either via argument or environment variable")

    # Set up clients
    bedrock = boto3.client('bedrock-runtime', region_name='us-west-2')
    s3 = boto3.client('s3')

    # Build the prompt and request body
    prompt = intro
    request_body = {
        "text_prompts": [{"text": prompt}],
        "cfg_scale": 10,
        "seed": 0,
        "steps": 50
    }

    # Call the Stable Diffusion model on Bedrock
    response = bedrock.invoke_model(
        modelId="stability.stable-diffusion-xl-v0",
        contentType="application/json",
        accept="application/json",
        body=json.dumps(request_body)
    )

    # Decode and extract image
    body_bytes = response['body'].read()
    output = json.loads(body_bytes.decode("utf-8"))
    base64_img = output["artifacts"][0]["base64"]
    image_bytes = base64.b64decode(base64_img)

    # Upload to S3
    image_key = "GEN-CONTENT/image.png"
    s3.put_object(Bucket=s3_bucket, Key=image_key, Body=image_bytes)

    write('xcom', 'imageLocation', image_key)
    write('xcom', 'textLocation', text_location)

def package_blog_and_image():
    text_location = read('xcom', 'textLocation')
    image_location = read('xcom', 'imageLocation')
    if not s3_bucket:
        raise ValueError("S3_BUCKET must be set")

    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    signer_client = boto3.client("s3")

    # Get blog markdown text
    text_obj = s3.get_object(Bucket=s3_bucket, Key=text_location)
    text_data = text_obj["Body"].read().decode("utf-8")

    # Get image bytes
    image_obj = s3.get_object(Bucket=s3_bucket, Key=image_location)
    image_data = image_obj["Body"].read()

    # Stitch image at top of blog markdown
    final_md = f"![Alt text](./image.png)\n\n{text_data}"
    final_html = markdown(final_md)

    # Create a zip in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("blog-final.md", final_md)
        zip_file.writestr("blog-final.html", final_html)
        zip_file.writestr("image.png", image_data)

    # Prepare to upload ZIP
    zip_key = f"GEN-CONTENT/{uuid.uuid4()}.zip"
    s3.put_object(Bucket=s3_bucket, Key=zip_key, Body=zip_buffer.getvalue())

    # Generate signed URL for download
    signed_url = signer_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': s3_bucket, 'Key': zip_key},
        ExpiresIn=3600
    )

    write('xcom', 'signed_url', signed_url)




dag = DAG(
    dag_id='stock',
    description='Buy or sell stock',
    schedule_interval=None
)


t1 = FusedPythonOperator(
    task_id="gen_block",
    python_callable=generate_blog_with_bedrock,
    dag=dag,
    provide_context=True,
)

t2 = FusedPythonOperator(
    task_id="gen_image",
    python_callable=generate_image_from_intro,
    dag=dag,
    provide_context=True,
)

t3 = FusedPythonOperator(
    task_id="package",
    python_callable=package_blog_and_image,
    dag=dag,
    provide_context=True,
)



t1 >> t2 >> t3

#fused_dag = create_optimized_dag_integer_programming(dag, None, None, 1)
