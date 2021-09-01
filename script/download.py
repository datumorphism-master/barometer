import json
import time
from typing import DefaultDict

import click
from durst.data.fetch_web import get_page_content
from loguru import logger


def get_lane(from_country, to_country, api_base=None):

    if api_base is None:
        api_base = "https://www.timocom.co.uk/WWW/modules/controller/Barometer.cfc"

    api = f"{api_base}?method=getRemoteBarometerData&wsdl&returnFormat=json&fromCountry={from_country}&toCountry={to_country}"

    logger.debug(f"Fetching {api}")

    result = get_page_content(api)

    if result["status"] != 200:
        logger.error(f"status: {result['status']}; could not download data")
    else:
        res = result["content"].json()
        res["FROM"] = from_country
        res["TO"] = to_country

        return res



@click.command()
@click.option("--api_base", "-api", default="https://www.timocom.co.uk/WWW/modules/controller/Barometer.cfc", help="Base URL of API")
@click.option("--config_file", "-cf", default=None, help="config file")
@click.option("--from_countries", "-f", default=["DE", "PL"], multiple=True, help="From countries")
@click.option("--to_countries", "-t",  default=["DE", "PL"], multiple=True, help="To countries")
@click.option("--combine", "-c", default="cross", help="How to combine the from and to lists; cross, zip")
@click.option("--wait", "-s", default=3, help="Waiting seconds between queries")
@click.option("--dump", "-d", default="data/timocom_barometer.json", help="Target file")
def main(from_countries, to_countries, dump, config_file, combine, api_base, wait):

    if config_file:
        with open(config_file, "r") as fp:
            config = json.load(fp)

        if not config.get("from_countries"):
            raise ValueError("from_countries not defined in config")
        else:
            from_countries = config["from_countries"]
        if not config.get("to_countries"):
            raise ValueError("to_countries not defined in config")
        else:
            to_countries = config["to_countries"]
    else:
        from_countries = ["DE", "PL"]
        to_countries = ["DE", "PL"]

    if combine == "cross":
        all_lanes = ((f, t) for f in from_countries for t in to_countries)
    elif combine == "zip":
        all_lanes = list(zip(from_countries, to_countries))
    else:
        raise ValueError(f"combine should be one of: cross, zip; {combine}")

    bm_all = []

    for ft in all_lanes:
        bm = get_lane(ft[0], ft[1], api_base=api_base)
        bm_all.append(bm)
        time.sleep(wait)

    logger.debug(bm_all)

    logger.debug("Dumping data")
    with open(dump, "w") as fp:
        json.dump(bm_all, fp, indent=4)

    logger.info(f"Dumped data to {dump}")


if __name__ == "__main__":
    main()
