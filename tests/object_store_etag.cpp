#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>

#include "storage/object_store.h"

// The ETag response header must be matched case-insensitively: HTTP header
// names are case-insensitive and HTTP/2 (e.g. GCS) delivers them lowercased.
// A byte-exact "ETag:" match would leave etag_ empty on HTTP/2, silently
// downgrading the term-file compare-and-swap to an unconditional PUT.
TEST_CASE("ParseETagHeader matches the header name case-insensitively",
          "[object_store]")
{
    using eloqstore::AsyncHttpManager;

    // HTTP/1.1 style (capitalized, quoted).
    auto h1 = AsyncHttpManager::ParseETagHeader("ETag: \"abc123\"\r\n");
    REQUIRE(h1.has_value());
    REQUIRE(*h1 == "abc123");

    // HTTP/2 style (lowercased) -- the case this fix targets.
    auto h2 = AsyncHttpManager::ParseETagHeader("etag: \"abc123\"\r\n");
    REQUIRE(h2.has_value());
    REQUIRE(*h2 == "abc123");

    // Mixed case, tab separator, unquoted weak validator.
    auto h3 = AsyncHttpManager::ParseETagHeader("Etag:\tW/xyz\r\n");
    REQUIRE(h3.has_value());
    REQUIRE(*h3 == "W/xyz");

    // Non-ETag header is ignored.
    REQUIRE_FALSE(
        AsyncHttpManager::ParseETagHeader("Content-Length: 5\r\n").has_value());

    // ETag header with no value.
    REQUIRE_FALSE(AsyncHttpManager::ParseETagHeader("etag:\r\n").has_value());

    // Shorter than the prefix.
    REQUIRE_FALSE(AsyncHttpManager::ParseETagHeader("eta").has_value());
}
