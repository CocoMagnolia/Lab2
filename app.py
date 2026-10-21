import csv
import glob
import os

from flask import Flask, jsonify, request

app = Flask(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_USERS = ["214328887", "107830991"]                 # 题目指定展示的两个用户


def _search_dirs():
    d = HERE
    for _ in range(3):
        yield d
        parent = os.path.dirname(d)
        if parent == d:
            break
        d = parent


def _candidates():
    paths = [os.environ.get("RESULTS_FILE", "")]
    for d in _search_dirs():
        paths += [
            os.path.join(d, "output", "twitter_counts.csv"),
            os.path.join(d, "twitter_result", "part-00000.csv"),
            os.path.join(d, "results", "part-00000.csv"),
        ]
    return paths


CANDIDATES = _candidates()


def _iter_files():
    for path in CANDIDATES:
        if path and os.path.isfile(path):
            yield path
    for pattern in ("results/*.csv", "twitter_result/*.csv"):
        for path in sorted(glob.glob(os.path.join(HERE, pattern))):
            yield path


def load_results():
    for path in _iter_files():
        stats = {}
        with open(path, encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                # Spark 的 part 文件列名可能是 _c0/_c1/_c2，做兼容
                user = row.get("user") or row.get("userId") or row.get("_c0")
                if not user:
                    continue
                fol = row.get("follower") or row.get("followers") or row.get("_c1") or 0
                fee = row.get("followee") or row.get("_c2") or 0
                stats[str(user)] = {"followers": int(float(fol)), "followees": int(float(fee))}
        if stats:
            print(f"Loaded {len(stats)} users from {path}")
            return stats
    print("WARNING: 没找到结果文件，接口会返回空。先跑 analyze.py。")
    return {}


STATS = load_results()


@app.get("/health")
def health():
    return jsonify(status="ok", users=len(STATS))


@app.get("/stats/<user_id>")
def one_user(user_id):
    entry = STATS.get(user_id)
    if entry is None:
        return jsonify(error=f"user {user_id} not found"), 404
    return jsonify(userId=user_id, **entry)


@app.get("/stats")
def many_users():
    requested = [u.strip() for u in request.args.get("users", "").split(",") if u.strip()]
    ids = requested or DEFAULT_USERS
    return jsonify(
        results={u: STATS[u] for u in ids if u in STATS},
        missing=[u for u in ids if u not in STATS],
    )


for rule, endpoint in (
    ("/api/health", "health"),
    ("/api/stats", "many_users"),
    ("/api/stats/<user_id>", "one_user"),
):
    app.add_url_rule(rule, endpoint=f"{endpoint}_api", view_func=app.view_functions[endpoint])


@app.get("/")
def index():
    path = os.path.join(HERE, "index.html")
    with open(path, encoding="utf-8") as fh:
        return fh.read()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")), debug=False)
