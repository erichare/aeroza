// Flat ESLint config for the @aeroza/console workspace.
//
// Hand-rolled rather than extending `eslint-config-next` because the
// preset bundles a Babel parser path (`next/dist/compiled/babel/eslint-parser`)
// that resolves only when `eslint-config-next` and `next` live in the
// *same* node_modules tree — npm workspaces hoist these into different
// trees, so the bundled preset breaks. Hand-rolling sidesteps the issue
// and keeps the rule set focused on bugs we actually want to catch.
//
// Rule set:
//   • @typescript-eslint/recommended — type-safety hygiene.
//   • react-hooks/rules-of-hooks + exhaustive-deps — the bug class
//     `next/core-web-vitals` exists primarily to enforce.
//   • react/jsx-uses-react + jsx-uses-vars — JSX scope correctness.
//   • no-console — library-style discipline (warn / error allowed).
//
// next-specific rules (e.g. `@next/next/no-html-link-for-pages`) are
// dropped — we only have one public app, and the rules they replace
// are easy to spot in review.

import js from "@eslint/js";
import tsPlugin from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";
import reactPlugin from "eslint-plugin-react";
import reactHooksPlugin from "eslint-plugin-react-hooks";
import globals from "globals";

export default [
  {
    ignores: [
      ".next/**",
      "node_modules/**",
      "playwright-report/**",
      "test-results/**",
      "next-env.d.ts",
    ],
  },
  js.configs.recommended,
  {
    files: ["**/*.{ts,tsx,js,jsx,mjs,cjs}"],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: 2022,
        sourceType: "module",
        ecmaFeatures: { jsx: true },
      },
      globals: {
        ...globals.browser,
        ...globals.node,
        React: "readonly",
      },
    },
    settings: {
      react: { version: "detect" },
    },
    plugins: {
      "@typescript-eslint": tsPlugin,
      react: reactPlugin,
      "react-hooks": reactHooksPlugin,
    },
    rules: {
      // TypeScript handles undefined-symbol detection itself (and knows
      // about lib types like `RequestInit` and DOM globals that ESLint
      // would otherwise flag).
      "no-undef": "off",
      // Base no-unused-vars conflicts with TS variant on type-only params.
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          ignoreRestSiblings: true,
        },
      ],
      "@typescript-eslint/no-explicit-any": "warn",

      // The two react-hooks rules are why most teams pick up
      // `next/core-web-vitals`; we surface them directly.
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",

      // JSX scope — without these, defining a JSX-only component
      // triggers no-unused-vars on the imported React or component.
      "react/jsx-uses-react": "error",
      "react/jsx-uses-vars": "error",

      // Library-style console discipline (warn / error escape hatch).
      "no-console": ["warn", { allow: ["warn", "error"] }],
    },
  },
];
