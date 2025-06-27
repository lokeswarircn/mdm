"""get glue parameters"""

try:
    # Try importing AWS Glue libraries first
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    is_glue = True
except ImportError:
    # Fall back to regular PySpark for local development
    is_glue = False


class Params(object):
    """Helper class which retrieves job input parameters"""

    def __init__(self, command_line_args):
        self.command_line_args = command_line_args

    def get(self, name, default_value=None):
        """Get parameter from command line arguments"""
        if default_value is not None:
            options = {name: self.__get_default(name, default_value)}
        else:
            options = getResolvedOptions(self.command_line_args, [name])

        return options[name]

    def __get_default(self, name, default_value):
        """get the default value"""
        if f'--{name}' not in self.command_line_args:
            return default_value

        index_val = self.command_line_args.index(f'--{name}')

        if len(self.command_line_args) > index_val + 1 and self.command_line_args[index_val + 1].startswith("--"):
            return default_value
        else:
            return self.command_line_args[index_val + 1]
