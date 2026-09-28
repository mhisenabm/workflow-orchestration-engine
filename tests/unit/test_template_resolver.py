import pytest

from apps.engine.application.template_resolver import TemplateResolutionError, TemplateResolver


@pytest.fixture
def resolver() -> TemplateResolver:
    return TemplateResolver()


@pytest.fixture
def outputs() -> dict:
    return {
        "input": {"customer_id": 42, "enabled": True},
        "user": {"profile": {"name": "John"}},
        "posts": {"items": [{"id": 1}, {"id": 2}], "count": 2, "optional": None},
    }


def test_full_template_preserves_json_types(resolver: TemplateResolver, outputs: dict) -> None:
    value = resolver.resolve(
        {
            "items": "{{ posts.items }}",
            "count": "{{ posts.count }}",
            "enabled": "{{ input.enabled }}",
            "optional": "{{ posts.optional }}",
        },
        outputs,
        {"input", "posts"},
    )
    assert value == {
        "items": [{"id": 1}, {"id": 2}],
        "count": 2,
        "enabled": True,
        "optional": None,
    }


def test_nested_and_embedded_templates(resolver: TemplateResolver, outputs: dict) -> None:
    value = resolver.resolve(
        ["Hello {{ user.profile.name }}", {"text": "Found {{ posts.count }} posts"}],
        outputs,
        {"user", "posts"},
    )
    assert value == ["Hello John", {"text": "Found 2 posts"}]


def test_object_in_embedded_string_is_json(resolver: TemplateResolver, outputs: dict) -> None:
    value = resolver.resolve("Posts: {{ posts.items }}", outputs, {"posts"})
    assert value == 'Posts: [{"id":1},{"id":2}]'


@pytest.mark.parametrize(
    "value, message",
    [
        ("{{ missing.value }}", "not an ancestor"),
        ("{{ user.missing }}", "was not found"),
        ("{{ user.profile.name", "malformed"),
    ],
)
def test_resolution_failures(
    resolver: TemplateResolver, outputs: dict, value: str, message: str
) -> None:
    with pytest.raises(TemplateResolutionError, match=message):
        resolver.resolve(value, outputs, {"user"})
