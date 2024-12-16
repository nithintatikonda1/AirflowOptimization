import inspect
import ast
import astor
import textwrap

PUSH_PATTERN = r"[A-Za-z0-9_]*\.xcom_push\(.*key\s*=\s*[\'\"]([^\'^\"]+)[\'\"].*value\s*=\s*(.*)[\s,]*.*\)"
PULL_PATTERN = r"[A-Za-z0-9_]*\.xcom_pull\(.*key\s*=\s*[\'\"]([^\'^\"]+)[\'\"].*\)"

def get_function_definition_lines(func):
    """ 
    Given a function, obtains a list of the function definition's lines

    Args:
        func (function): The function whose definition to obtain

    Returns:
        list[str]: The function definition as a list of strings.
    """
    string = inspect.getsource(func)
    string = textwrap.dedent(string)
    tree = ast.parse(string)
    string = astor.to_source(tree)
    lst = string.split('\n')
    return lst