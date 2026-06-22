import textwrap

import snakemd


def readme_feature(
    doc: snakemd.Document,
    main_header: str,
) -> snakemd.Document:

    # Some Specific information

    doc.add_heading(
        text=main_header,
        level=1,
    )

    # Logo

    doc.add_paragraph(
        snakemd.Inline(
            text=textwrap.dedent("""\
                Logo Flamenco\
                """),
            image="https://flamenco.blender.org/brand.svg",
            link="https://flamenco.blender.org/",
        ).__str__()
    )

    doc.add_paragraph(text=textwrap.dedent("""\
            Please visit the
            [Blender Flamenco](https://flamenco.blender.org/)
            landing page for more information.\
            """))

    doc.add_heading(
        text="Nvidia GPU Rendering",
        level=2,
    )

    doc.add_paragraph(text=textwrap.dedent("""\
            Requirements:\
            """))

    doc.add_unordered_list(
        [
            "[NVIDIA Container Toolkit (included in OpenStudioLandscapes-Flamenco-Worker image)](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/index.html)",
            "`nvidia` Docker runtime (see below)",
            "[`enable_gpu_in_blender_pref.py`](https://github.com/michimussato/OpenStudioLandscapes-Flamenco/blob/main/.payload/config/enable_gpu_in_blender_pref.py) (see `--python-expr` in `blender --help`)",
            "In `.blend` file Render Properties, set `Device=GPU Compute`"  # Todo: could that be scripted?
        ]
    )

    doc.add_paragraph(text=textwrap.dedent("""\
            To enable Nvidia GPU rendering in Docker 
            containers, the Docker daemon must be configured 
            accordingly. Add the following keys/values to 
            `/etc/docker/daemon.json` on each host Flamenco Worker 
            is running on inside a container:\
            """))

    doc.add_code(
        code=textwrap.dedent(
            """\
            {
              "runtimes": {
                "nvidia": {
                  "args": [],
                  "path": "nvidia-container-runtime"
                }
              }
            }\
"""
        ),
        lang="json",
    )

    doc.add_paragraph(text=textwrap.dedent("""\
            And restart the Docker daemon:\
            """))

    doc.add_code(
        code=textwrap.dedent(
            """\
            sudo systemctl restart docker docker.socket\
"""
        ),
        lang="shell",
    )

    doc.add_paragraph(text=textwrap.dedent("""\
            Some additional references:\
            """))

    doc.add_unordered_list(
        [
            "[sweettastebuds/flamenco-docker-server](https://github.com/sweettastebuds/flamenco-docker-server)",
            "[Maxattax97/docker-flamenco](https://github.com/Maxattax97/docker-flamenco)",
            "[Rendering on command-line with GPU?](https://blender.stackexchange.com/a/256665/152092)",
        ]
    )

    doc.add_horizontal_rule()

    return doc


if __name__ == "__main__":
    pass
