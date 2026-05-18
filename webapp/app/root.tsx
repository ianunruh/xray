import "@mantine/core/styles.css";
import "@mantine/notifications/styles.css";

import {
  AppShell,
  ColorSchemeScript,
  Group,
  MantineProvider,
  NavLink as MantineNavLink,
  Title,
  createTheme,
  mantineHtmlProps,
} from "@mantine/core";
import { Notifications } from "@mantine/notifications";
import {
  Links,
  Meta,
  NavLink,
  Outlet,
  Scripts,
  ScrollRestoration,
} from "react-router";

const theme = createTheme({
  fontFamily:
    "'SF Mono', 'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace",
  fontFamilyMonospace:
    "'SF Mono', 'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace",
});

const NAV = [
  { to: "/trading", label: "Trading" },
  { to: "/traders", label: "Traders" },
  { to: "/markets", label: "Markets" },
  { to: "/diagnostics", label: "Diagnostics" },
  { to: "/chain", label: "Chain" },
  { to: "/projections", label: "Projections" },
];

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" {...mantineHtmlProps}>
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>xray</title>
        <ColorSchemeScript defaultColorScheme="dark" />
        <Meta />
        <Links />
      </head>
      <body>
        <MantineProvider defaultColorScheme="dark" theme={theme}>
          <Notifications position="top-right" />
          {children}
        </MantineProvider>
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

export default function Root() {
  return (
    <AppShell header={{ height: 50 }} padding="md">
      <AppShell.Header p="xs">
        <Group gap="md" h="100%">
          <Title order={4}>xray</Title>
          <Group gap={4}>
            {NAV.map((n) => (
              <MantineNavLink
                key={n.to}
                component={NavLink}
                to={n.to}
                label={n.label}
                style={{ width: "auto", padding: "4px 10px" }}
              />
            ))}
          </Group>
        </Group>
      </AppShell.Header>
      <AppShell.Main>
        <Outlet />
      </AppShell.Main>
    </AppShell>
  );
}
