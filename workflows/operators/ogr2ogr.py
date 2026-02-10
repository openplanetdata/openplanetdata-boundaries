"""Custom Airflow operator for running ogr2ogr inside a GDAL Docker container."""

import os
import shlex

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from openplanetdata.airflow.defaults import DOCKER_MOUNT

DOCKER_USER = f"{os.getuid()}:{os.getgid()}"


class Ogr2OgrOperator(DockerOperator):
    """Run ogr2ogr inside a GDAL Docker container."""

    def __init__(
        self,
        *,
        args: list[str],
        image: str = "ghcr.io/osgeo/gdal:ubuntu-full-latest",
        **kwargs,
    ):
        cmd = shlex.join(["ogr2ogr", *args])
        kwargs.setdefault("auto_remove", "success")
        kwargs.setdefault("force_pull", True)
        kwargs.setdefault("mount_tmp_dir", False)
        kwargs.setdefault("mounts", [Mount(**DOCKER_MOUNT)])
        kwargs.setdefault("user", DOCKER_USER)
        super().__init__(
            command=f"bash -c {shlex.quote(cmd)}",
            image=image,
            **kwargs,
        )
