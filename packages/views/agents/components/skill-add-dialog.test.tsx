import { describe, it, expect, vi, beforeEach } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { I18nProvider } from "@multica/core/i18n/react";
import type { Agent } from "@multica/core/types";
import enCommon from "../../locales/en/common.json";
import enAgents from "../../locales/en/agents.json";

const TEST_RESOURCES = {
  en: { common: enCommon, agents: enAgents },
};

const mockListSkills = vi.hoisted(() => vi.fn());
const mockListLabels = vi.hoisted(() => vi.fn());
const mockSetAgentSkills = vi.hoisted(() => vi.fn());

vi.mock("@multica/core/api", () => ({
  api: {
    listSkills: () => mockListSkills(),
    listLabels: (...args: unknown[]) => mockListLabels(...args),
    setAgentSkills: (...args: unknown[]) => mockSetAgentSkills(...args),
  },
}));

vi.mock("@multica/core/hooks", () => ({
  useWorkspaceId: () => "ws-1",
}));

import { SkillAddDialog } from "./skill-add-dialog";

const LABEL_LARK = {
  id: "label-lark",
  workspace_id: "ws-1",
  resource_type: "skill",
  name: "lark",
  description: "",
  color: "#3b82f6",
  usage_count: 2,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

const SKILL = (id: string, name: string, labelIds: string[]) => ({
  id,
  workspace_id: "ws-1",
  name,
  description: "",
  config: {},
  created_by: "user-1",
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  labels: labelIds.map((labelId) => ({
    ...LABEL_LARK,
    id: labelId,
    name: labelId === "label-lark" ? "lark" : labelId,
  })),
});

function renderDialog(agentSkills: ReturnType<typeof SKILL>[] = []) {
  const agent = {
    id: "agent-1",
    skills: agentSkills,
  } as unknown as Agent;
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <I18nProvider locale="en" resources={TEST_RESOURCES}>
      <QueryClientProvider client={queryClient}>
        <SkillAddDialog agent={agent} open onOpenChange={vi.fn()} />
      </QueryClientProvider>
    </I18nProvider>,
  );
}

describe("SkillAddDialog label filter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockListSkills.mockResolvedValue([
      SKILL("s1", "lark-im", ["label-lark"]),
      SKILL("s2", "lark-doc", ["label-lark"]),
      SKILL("s3", "ui-design", ["label-ui"]),
    ]);
    mockListLabels.mockResolvedValue({
      labels: [LABEL_LARK, { ...LABEL_LARK, id: "label-ui", name: "ui" }],
      total: 2,
    });
    mockSetAgentSkills.mockResolvedValue({});
  });

  it("adds a whole label group in one click", async () => {
    const user = userEvent.setup();
    renderDialog();
    expect(await screen.findByText("lark-im")).toBeInTheDocument();

    await user.click(screen.getByRole("combobox", { name: /Filter by label/i }));
    await user.click(await screen.findByRole("option", { name: "lark" }));

    // Only the lark group is visible now, and select-all targets it.
    expect(screen.queryByText("ui-design")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Add all \(2\)/i }));

    fireEvent.click(screen.getByRole("button", { name: /^Add 2 skills$/i }));
    await waitFor(() => expect(mockSetAgentSkills).toHaveBeenCalled());
    expect(mockSetAgentSkills).toHaveBeenCalledWith("agent-1", {
      skill_ids: ["s1", "s2"],
    });
  });

  it("keeps every skill listed when no label is chosen", async () => {
    renderDialog();
    expect(await screen.findByText("lark-im")).toBeInTheDocument();
    expect(screen.getByText("lark-doc")).toBeInTheDocument();
    expect(screen.getByText("ui-design")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Add all \(3\)/i }),
    ).toBeInTheDocument();
  });
});
