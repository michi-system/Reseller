# Codex共同開発オンボーディング（別Mac / 別アカウント）

最終更新: 2026-02-22 (JST)

この手順は、`michi-system/ebayminer` を別アカウントのCodex環境から編集するための最短手順です。  
対象は「GitHubに不慣れでも、同じ手順で参加できること」です。

## 1. 参加前にオーナーがやること
1. GitHubで共同作業者をコラボレーター追加する。
2. ブランチ保護ルールを確認する（推奨: `main` 直push禁止、PR経由）。
3. Supabase運用の場合は、共有する値を決める。
- 共有してよい: `SUPABASE_URL`
- 共有してよい: `SUPABASE_DB_URL`（必要なら）
- 共有注意: `SUPABASE_SERVICE_ROLE_KEY`（最小人数のみ）

## 2. 参加者が最初にやること（別Mac）
1. リポジトリをcloneする。

```bash
git clone https://github.com/michi-system/ebayminer.git
cd ebayminer
```

2. Python仮想環境を作る。

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
```

3. `.env.local` を作成する。

```bash
cp .env.example .env.local
```

4. `.env.local` を編集する（最低限）。
- Supabase運用: `DB_BACKEND=postgres`
- ローカル運用: `DB_BACKEND=sqlite`
- Supabase運用時は `SUPABASE_DB_URL` を設定

5. PostgreSQL運用に必要なライブラリを入れる。

```bash
.venv/bin/python -m pip install "psycopg[binary]"
```

## 3. 動作確認（参加直後）
1. APIを起動する。

```bash
python3 scripts/run_api.py --host 127.0.0.1 --port 8012
```

2. 別ターミナルで確認する。

```bash
curl http://127.0.0.1:8012/healthz
```

3. `{"ok": true}` が返れば参加準備完了。

## 4. 日々の作業ルール（最小）
1. 作業前に `main` を最新化する。
2. ブランチを切って実装する。
3. PRでレビュー後にマージする。
4. 作業終了時は日報を出す。

```bash
python3 scripts/generate_daily_report.py --date YYYY-MM-DD --operator @github_id
```

## 5. 作業者同定ルール
作業者は次の3点で同定する。
- `GitHub Actor`
- `Git Author`
- `Daily Report Owner`

不一致がある場合は、日報の `Risks / Follow-ups` に理由を記録する。

## 6. トラブル時の復帰
1. Supabase運用で不具合が出たら、`DB_BACKEND=sqlite` に戻す。
2. 必要ならローカル復元を実行する。

```bash
python3 scripts/restore_local_checkpoint.py --apply
```

3. 復帰理由を `docs/WORKBOARD.md` の `Decision Log` に追記する。
