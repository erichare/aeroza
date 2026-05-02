import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Aeroza · Dev Console",
  description:
    "Live test harness for the Aeroza weather-intelligence API: alerts SSE stream, MRMS catalog, system health.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="font-sans antialiased">{children}</body>
    </html>
  );
}
