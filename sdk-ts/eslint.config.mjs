// Flat ESLint config for the @aeroza/sdk workspace.
//
// SDK is a plain TypeScript library with no framework. Minimal rule set
// focused on type-safety hygiene: ban `any`, surface unused symbols,
// and warn on console output (the SDK is library code; logs should
// flow through caller-supplied hooks, not console).

import js from "@eslint/js";
import tsPlugin from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";
import globals from "globals";

export default [
  {
    ignores: ["dist/**", "node_modules/**", "coverage/**", "*.config.mjs"],
  },
  js.configs.recommended,
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 2022,
        sourceType: "module",
      },
      // SDK targets both Node 18+ and modern browsers — include both
      // global sets so URL / URLSearchParams / Response / RequestInit
      // are recognised.
      globals: {
        ...globals.browser,
        ...globals.node,
      },
    },
    plugins: {
      "@typescript-eslint": tsPlugin,
    },
    rules: {
      // TypeScript handles undefined-symbol detection itself (and knows
      // about lib types like `RequestInit` that ESLint can't see).
      "no-undef": "off",
      // Base no-unused-vars conflicts with the TS variant on type-only
      // params; defer to the TS rule.
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/no-explicit-any": "warn",
      "no-console": ["warn", { allow: ["warn", "error"] }],
    },
  },
];
