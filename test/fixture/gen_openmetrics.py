"""Write the deterministic OpenMetrics backfill used by the live Prometheus fixture.

    python3 test/fixture/gen_openmetrics.py > test/fixture/data.om

Every series covers 2026-01-01T00:00:00Z .. 06:00:00Z at a 15 s interval
(1,441 samples, index i = 0..1440), so SQL tests can assert exact values:

  fixture_temperature_celsius   gauge   node-a: 20 + i%10, node-b: 30 + i%5, node-c: 10 + 0.5*(i%2)
  fixture_requests_total        counter node-a GET 200: 10*i, node-a POST 500: i,
                                        node-b GET 200: 5*i (resets to 0 at i = 720)
  fixture_request_duration_seconds histogram (classic) node-a: le 0.1/0.5/1/+Inf = i, 3i, 4i, 5i
  fixture_special               gauge   NaN, +Inf, -Inf
  fixture_gappy                 gauge   1, absent for 01:00 <= t < 02:00
  fixture_escape_test           gauge   label value with a quote and a backslash
"""

START = 1767225600  # 2026-01-01T00:00:00Z
STEP = 15
N = 1441


def ts(i):
    return START + i * STEP


def lbl(**kv):
    def esc(v):
        return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return "{" + ",".join(f'{k}="{esc(v)}"' for k, v in kv.items()) + "}"


def fmt(v):
    if isinstance(v, str):
        return v
    if float(v).is_integer():
        return str(int(v))
    return repr(float(v))


out = []
w = out.append

w("# TYPE fixture_temperature_celsius gauge")
w("# UNIT fixture_temperature_celsius celsius")
w("# HELP fixture_temperature_celsius Fixture room temperature in degrees Celsius.")
temps = [
    (dict(job="fixture", instance="node-a:9100", site="ams", env="prod"), lambda i: 20 + i % 10),
    (dict(job="fixture", instance="node-b:9100", site="ams", env="prod"), lambda i: 30 + i % 5),
    (dict(job="fixture", instance="node-c:9100", site="nyc", env="staging"), lambda i: 10 + 0.5 * (i % 2)),
]
for labels, f in temps:
    for i in range(N):
        w(f"fixture_temperature_celsius{lbl(**labels)} {fmt(f(i))} {ts(i)}")

w("# TYPE fixture_requests counter")
w("# HELP fixture_requests Fixture HTTP requests served.")
reqs = [
    (dict(job="fixture", instance="node-a:9100", method="GET", code="200"), lambda i: 10 * i),
    (dict(job="fixture", instance="node-a:9100", method="POST", code="500"), lambda i: i),
    (dict(job="fixture", instance="node-b:9100", method="GET", code="200"), lambda i: 5 * (i if i < 720 else i - 720)),
]
for labels, f in reqs:
    for i in range(N):
        w(f"fixture_requests_total{lbl(**labels)} {fmt(f(i))} {ts(i)}")

w("# TYPE fixture_request_duration_seconds histogram")
w("# UNIT fixture_request_duration_seconds seconds")
w("# HELP fixture_request_duration_seconds Fixture request latency.")
base = dict(job="fixture", instance="node-a:9100")
for i in range(N):
    for le, mult in (("0.1", 1), ("0.5", 3), ("1.0", 4), ("+Inf", 5)):
        w(f"fixture_request_duration_seconds_bucket{lbl(**base, le=le)} {mult * i} {ts(i)}")
    w(f"fixture_request_duration_seconds_count{lbl(**base)} {5 * i} {ts(i)}")
    w(f"fixture_request_duration_seconds_sum{lbl(**base)} {fmt(2.0 * i)} {ts(i)}")

w("# TYPE fixture_special gauge")
w("# HELP fixture_special Fixture non-finite values.")
for kind, v in (("nan", "NaN"), ("neginf", "-Inf"), ("posinf", "+Inf")):
    for i in range(N):
        w(f"fixture_special{lbl(job='fixture2', instance='special:1', kind=kind)} {v} {ts(i)}")

w("# TYPE fixture_gappy gauge")
w("# HELP fixture_gappy Fixture series with a one-hour gap.")
for i in range(N):
    if 240 <= i < 480:
        continue
    w(f"fixture_gappy{lbl(job='fixture2', instance='gap:1')} 1 {ts(i)}")

w("# TYPE fixture_escape_test gauge")
w("# HELP fixture_escape_test Fixture label value that needs escaping.")
for i in range(N):
    w(f"fixture_escape_test{lbl(job='fixture2', instance='esc:1', path='/a\"b' + chr(92) + 'c')} 1 {ts(i)}")

w("# EOF")
print("\n".join(out))
