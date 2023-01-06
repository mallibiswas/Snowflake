import boto3
import json


def get_json_secret_value(name):
    """ Gets json secret from aws as an object.

    :type name: string
    :param name: The name of the secret to retrive. This can be the full arn or the name.

    :return: secrets as a dictionary
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    secret = client.get_secret_value(
        SecretId=name,
    )
    return json.loads(secret.get('SecretString'))
