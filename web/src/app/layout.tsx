import type { Metadata } from "next";
import { Inter } from "next/font/google";
import { ColorSchemeScript, MantineProvider, createTheme } from '@mantine/core';

import "./globals.css";
import '@mantine/core/styles.css';
import '@mantine/notifications/styles.css';
import MainLayout from "../components/MainLayout";
import { AuthProvider } from "../components/AuthProvider";
import { Notifications } from '@mantine/notifications';

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Nagare | Dashboard",
  description: "Minimal Go Workflow Engine",
};

const theme = createTheme({
  primaryColor: 'blue',
  fontFamily: inter.style.fontFamily,
  defaultRadius: 'md',
  headings: {
    fontFamily: inter.style.fontFamily,
    fontWeight: '700',
    sizes: {
      h1: { fontSize: '2.5rem' },
      h2: { fontSize: '2rem' },
      h3: { fontSize: '1.5rem' },
    },
  },
  components: {
    Card: {
      defaultProps: {
        radius: 'md',
        withBorder: true,
      },
      styles: {
        root: {
          backgroundColor: 'var(--panel-bg)',
          borderColor: 'var(--border-color)',
        }
      }
    },
    Button: {
      defaultProps: {
        radius: 'md',
        size: 'sm',
      },
    },
    Table: {
      defaultProps: {
        striped: true,
        highlightOnHover: true,
      }
    }
  }
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
          <AuthProvider>
            <MainLayout>{children}</MainLayout>
          </AuthProvider>
        </MantineProvider>
      </body>
    </html>
  );
}
