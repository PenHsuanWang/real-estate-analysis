from dagster import ConfigurableResource


class CsvPathResource(ConfigurableResource):
    """Holds the file system path to the raw transaction CSV."""
    path: str
