import type { Metadata } from "next";
import { Fraunces, Inter, JetBrains_Mono } from "next/font/google";

import "./globals.css";

import { SiteNav } from "@/components/SiteNav";

// Body sans — Inter for legibility. The cv11/ss01/ss03 stylistic alternates
// engaged in globals.css give it a slightly more editorial feel than vanilla.
const inter = Inter({
  subsets: ["latin"],
  variable: "--font-sans",
  display: "swap",
});

// Display serif — Fraunces, a contemporary serif with warm character. Used for
// h1 / h2 via the `.font-display` utility (mapped through tailwind.config.ts).
// Display=optional + weight=400/600 keeps the asset payload small.
const fraunces = Fraunces({
  subsets: ["latin"],
  weight: ["400", "600"],
  variable: "--font-display",
  display: "swap",
});

const jetbrains = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: {
    default: "Aeroza · Weather, but queryable.",
    template: "%s · Aeroza",
  },
  description:
    "Programmable weather intelligence: streaming APIs, geospatial queries, and " +
    "probabilistic nowcasting for modern applications.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html
      lang="en"
      className={[
        inter.variable,
        fraunces.variable,
        jetbrains.variable,
      ].join(" ")}
    >
      <body className="font-sans antialiased">
        <SiteNav />
        {children}
      </body>
    </html>
  );
}
