import type { Metadata } from "next";
import { Inter } from "next/font/google";
import { ColorSchemeScript, MantineProvider, createTheme } from '@mantine/core';

import "./globals.css";
import '@mantine/core/styles.css';
import '@mantine/notifications/styles.css';
import MainLayout from "../components/MainLayout";
import { Notifications } from '@mantine/notifications';

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Nagare | Dashboard",
  description: "Minimal Go Workflow Engine",
};

const theme = createTheme({
  primaryColor: 'cyan',
  fontFamily: inter.style.fontFamily,
});

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <ColorSchemeScript />
      </head>
      <body>
        <MantineProvider theme={theme} defaultColorScheme="dark">
          <Notifications position="top-right" zIndex={1000} />
          <MainLayout>{children}</MainLayout>
        </MantineProvider>
      </body>
    </html>
  );
}
