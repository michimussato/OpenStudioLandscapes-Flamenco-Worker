import textwrap

import snakemd


def readme_feature(doc: snakemd.Document) -> snakemd.Document:

    # Some Specific information

    doc.add_heading(
        text="Official Resources",
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

    return doc


if __name__ == "__main__":
    pass
