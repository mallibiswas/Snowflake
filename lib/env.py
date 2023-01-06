import re


def get_zenreach_prefix(name):
    """ Gets the zenreach environment prefix

    :type name: string
    :param name: The name of this airflow environment expected in format
        similar to "zd-uw2-instance"

    :return: string: eg. "zd-uw2-"
    """
    match = re.search("(z[dsp]-[a-zA-Z]+[0-9]+-)", name)

    return match[1]
