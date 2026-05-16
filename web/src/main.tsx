import "@mantine/core/styles.css";
import "@mantine/notifications/styles.css";
import { createRoot } from "react-dom/client";
import { MantineProvider, createTheme } from "@mantine/core";
import { Notifications } from "@mantine/notifications";
import { App } from "./App";

const theme = createTheme({
  fontFamily:
    "'SF Mono', 'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace",
  fontFamilyMonospace:
    "'SF Mono', 'Cascadia Code', 'Fira Code', 'JetBrains Mono', monospace",
});

createRoot(document.getElementById("root")!).render(
  <MantineProvider defaultColorScheme="dark" theme={theme}>
    <Notifications position="top-right" />
    <App />
  </MantineProvider>,
);
