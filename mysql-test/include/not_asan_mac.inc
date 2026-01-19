# mtr tests which do INSTALL PLUGIN foo; UNINSTALL PLUGIN foo; INSTALL PLUGIN foo;
# will get ODR violations in the second INSTALL.
# This is because the dynamic libraries are not unloaded, see man dlclose(3) on macOS.
let $have_asan_mac = `SELECT VERSION() LIKE '%asan%' AND CONVERT(@@version_compile_os using latin1) RLIKE '^(osx|macos)' = 1`;
if ($have_asan_mac)
{
  --skip Test requires 'not_asan_mac'
}
