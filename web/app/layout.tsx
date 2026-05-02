import type { Metadata } from "next";
import "./globals.css";

import { SiteNav } from "@/components/SiteNav";

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
    <html lang="en" className="dark">
      <body className="font-sans antialiased">
        <SiteNav />
        {children}
      </body>
    </html>
  );
}
