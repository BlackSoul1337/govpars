from procurement_parser.infrastructure.sources.zakup_sk.request_profiles import (
    request_profile_key,
)


def test_detail_profiles_are_shared_between_entity_ids() -> None:
    assert request_profile_key(
        "GET",
        "https://zakup.sk.kz/eprocsearch/api/external/lots/123",
    ) == request_profile_key(
        "GET",
        "https://zakup.sk.kz/eprocsearch/api/external/lots/999",
    )
    assert request_profile_key(
        "GET",
        "https://zakup.sk.kz/eprocsearch/api/external/4dv3rts/123",
    ) == request_profile_key(
        "GET",
        "https://zakup.sk.kz/eprocsearch/api/external/4dv3rts/999",
    )


def test_filter_profiles_remain_endpoint_specific() -> None:
    assert request_profile_key(
        "POST",
        "https://zakup.sk.kz/eprocsearch/api/external/lots/filter?page=0",
    ) == "POST /eprocsearch/api/external/lots/filter"
