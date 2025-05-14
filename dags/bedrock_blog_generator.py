from airflow import DAG
from airflow.operators.python import PythonOperator
from airflowfusion.operator import FusedPythonOperator
from airflowfusion.fuse import create_optimized_dag
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
import openai

s3_bucket = os.environ.get("S3_BUCKET")
openai.api_key = os.environ.get("OPENAI_API_KEY")
api_key = os.environ.get("OPENAI_API_KEY")
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')  # Set environment variables for credentials
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') 

def generate_blog_with_bedrock():
    input_text = "Website about dogs."
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
    prompt = (
    f"{input_text} Include a title, an introduction section, sub sections "
    "and a conclusion section, and output in Markdown. Respond in <response> XML tag."
)
    # Send prompt to OpenAI model (e.g., GPT-4)
    client = openai.OpenAI(api_key=api_key)
    completion = client.chat.completions.create(
        model="gpt-4",  # You can use "gpt-3.5-turbo" for GPT-3.5 if preferred
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=600,
        temperature=1,
        top_p=0.999,
        stop=["\n\nHuman:"],
    )

    # Extract and print the response from OpenAI
    completion = completion.choices[0].message.content
    print(completion)

    # Extract markdown content from Claude's response
    try:
        md_text = completion.split('<response>\n')[1].split('\n</response>')[0]
    except IndexError:
        raise ValueError("Claude response did not contain valid <response> tags.")
    
    if md_text[0] == '\n':
        md_text = md_text[1:]
    if md_text[-1] == '\n':
        md_text = md_text[:-1]

    # Save to S3
    s3_key = 'GEN-CONTENT/blog-text.md'
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=md_text.encode("utf-8"))

    # Extract intro text
    if '## Introduction' in md_text:
        print(md_text)
        get_intro = md_text.split('## Introduction\n')[1].split('\n##')[0]
    else:
        get_intro = md_text.split('\n\n')[1]

    if get_intro[0] == '\n':
        get_intro = get_intro[1:]
    if get_intro[-1] == '\n':
        get_intro = get_intro[:-1]

    write('xcom', 'intro', get_intro)
    write('xcom', 'textLocation', s3_key)

def generate_image_from_intro():
    intro = read('xcom', 'intro')
    text_location = read('xcom', 'textLocation')

    if not s3_bucket:
        raise ValueError("S3_BUCKET must be provided either via argument or environment variable")

    # Set up clients
    openai.api_key = os.getenv("OPENAI_API_KEY")  # Set your OpenAI API key (ensure it's stored securely)
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

    # Build the prompt and request body
    client = openai.OpenAI(api_key=api_key)
    prompt = intro

    # Call OpenAI's DALL·E to generate an image from the prompt
    response = client.images.generate(
        prompt=prompt,
        n=1,  # Number of images to generate
        size="1024x1024",  # Size of the generated image
        response_format="url"  # Get the image URL
    )

    # Extract the image URL from the response
    image_url = response.data[0].url

    # Download the image from the URL
    import requests
    image_data = requests.get(image_url).content

    # Upload to S3
    image_key = "GEN-CONTENT/image.png"
    s3.put_object(Bucket=s3_bucket, Key=image_key, Body=image_data)

    # Write the result back to XCom
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
    dag_id='bedrock_blog_generator',
    description='bedrock',
    schedule_interval=None
)


t1 = PythonOperator(
    task_id="gen_block",
    python_callable=generate_blog_with_bedrock,
    dag=dag,
    provide_context=True,
)

t2 = PythonOperator(
    task_id="gen_image",
    python_callable=generate_image_from_intro,
    dag=dag,
    provide_context=True,
)

t3 = PythonOperator(
    task_id="package",
    python_callable=package_blog_and_image,
    dag=dag,
    provide_context=True,
)



t1 >> t2 >> t3

#fused_dag = create_optimized_dag_integer_programming(dag, None, None, 1)
fused_dag = create_optimized_dag(dag, parallelize=False)
optimized_dag = create_optimized_dag(dag)
