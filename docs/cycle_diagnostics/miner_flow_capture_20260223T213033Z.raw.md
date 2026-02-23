# Miner探索 実測レポート（完全版）: 20260223T213033Z

## 0. 実行メタ
- カテゴリ: `watch`
- 開始(UTC): `2026-02-23T21:30:40.015000+00:00`
- 終了(UTC): `2026-02-23T21:30:40.198000+00:00`
- 実行時間: `183 ms`
- UIサンプル数: `0`
- バックエンドサンプル数: `0`
- UI遷移イベント数: `0`
- バックエンド遷移イベント数: `0`

## 1. fetchレスポンス要約
```json
{}
```

## 2. fetch hints
```json
[]
```

## 3. post queue (pending)
```json
{}
```

## 4. UI遷移イベント（全件）
```json
[]
```

## 5. バックエンド遷移イベント（全件）
```json
[]
```

## 6. UI時系列（全サンプル）
```json
[]
```

## 7. バックエンド時系列（全サンプル）
```json
[]
```

## 8. errors
```json
[
  {
    "t": 1771882240198,
    "phase": "fatal",
    "message": "ReferenceError: setTimeout is not defined",
    "stack": "ReferenceError: setTimeout is not defined\n    at evalmachine.<anonymous>:4:49\n    at new Promise (<anonymous>)\n    at wait (evalmachine.<anonymous>:4:24)\n    at evalmachine.<anonymous>:74:11\n    at async evalmachine.<anonymous>:3:26\n    at async /Users/tadamichikimura/.npm/_npx/31e32ef8478fbf80/node_modules/playwright/lib/mcp/browser/tools/runCode.js:67:7\n    at async waitForCompletion (/Users/tadamichikimura/.npm/_npx/31e32ef8478fbf80/node_modules/playwright/lib/mcp/browser/tools/utils.js:35:14)\n    at async Tab._raceAgainstModalStates (/Users/tadamichikimura/.npm/_npx/31e32ef8478fbf80/node_modules/playwright/lib/mcp/browser/tab.js:298:12)\n    at async Tab.waitForCompletion (/Users/tadamichikimura/.npm/_npx/31e32ef8478fbf80/node_modules/playwright/lib/mcp/browser/tab.js:308:5)\n    at async Object.handle (/Users/tadamichikimura/.npm/_npx/31e32ef8478fbf80/node_modules/playwright/lib/mcp/browser/tools/runCode.js:58:5)"
  }
]
```

## 9. 生データファイル
- result json: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.result.json`
- ui jsonl: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.ui.jsonl`
- backend jsonl: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.backend.jsonl`
- ui transitions: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.ui.transitions.json`
- backend transitions: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.backend.transitions.json`
