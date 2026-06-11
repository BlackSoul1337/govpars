from pydantic import SecretStr

from procurement_parser.infrastructure.network.session import RequestProfile


def test_request_profile_filters_transport_and_request_scoped_headers() -> None:
    profile = RequestProfile(
        method="POST",
        url_pattern="https://example.test/filter",
        static_headers={
            "Accept": "application/json",
            "Host": "example.test",
            "Content-Length": "100",
            ":authority": "example.test",
            "Sec-Ch-Ua": '"Chromium"',
        },
        session_headers={
            "Cookie": SecretStr("session=secret"),
            "tor": SecretStr("session-signature"),
        },
        request_scoped_header_names={"tor"},
    )

    assert profile.transferable_headers() == {
        "Accept": "application/json",
        "Cookie": "session=secret",
        ":authority": "example.test",
        "Sec-Ch-Ua": '"Chromium"',
    }
    assert profile.browser_transferable_headers() == {
        "Accept": "application/json",
    }
