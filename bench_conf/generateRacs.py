for i in range(64):
    with open(f"rac{i}.toml", "w") as f:
        f.write('''[general]
id = "rac"
config_dir = "bench_conf/"

[rac]
ctrl_addr = "127.0.0.1:33333"
addr = "127.0.0.1:PORT"
static = false

[log.console]
level = "debug"

[rac.static_algorithm]
file = ""
                '''.replace("PORT", str(20000 + i)))
