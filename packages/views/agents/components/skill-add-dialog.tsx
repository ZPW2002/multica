"use client";

import { useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ListFilter } from "lucide-react";
import type { Agent, SkillSummary } from "@multica/core/types";
import { api } from "@multica/core/api";
import { useWorkspaceId } from "@multica/core/hooks";
import { labelListOptions } from "@multica/core/labels/queries";
import {
  skillListOptions,
  workspaceKeys,
} from "@multica/core/workspace/queries";
import { Button } from "@multica/ui/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@multica/ui/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@multica/ui/components/ui/select";
import { useT } from "../../i18n";
import { SkillPickerList } from "./skill-picker-list";

const ALL_LABELS = "__all__";

/**
 * "Attach workspace skills to this agent." Multi-select with explicit
 * Confirm — earlier iterations attached on a single row click, which
 * meant the user couldn't tick several skills at once and the dialog
 * closed before they could review their choice.
 *
 * Already-attached skills are filtered out of the list entirely (vs.
 * showing them disabled). When there are no remaining workspace skills
 * to attach, the empty-state copy explains why, and the Confirm button
 * is naturally disabled because nothing can be selected.
 *
 * The label filter narrows the list to one group (e.g. a directory
 * import's shared label) and "select all" adds the whole visible group
 * in one click, so binding a 29-skill bundle is two clicks, not 29.
 */
export function SkillAddDialog({
  agent,
  open,
  onOpenChange,
}: {
  agent: Agent;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const { t } = useT("agents");
  const wsId = useWorkspaceId();
  const qc = useQueryClient();
  const { data: workspaceSkills = [], isLoading } = useQuery(skillListOptions(wsId));
  const { data: skillLabels = [] } = useQuery(labelListOptions(wsId, "skill"));
  const [saving, setSaving] = useState(false);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [labelId, setLabelId] = useState<string>(ALL_LABELS);

  const attachedIds = useMemo(
    () => new Set(agent.skills.map((s) => s.id)),
    [agent.skills],
  );
  // Hide attached skills outright — the dialog is for adding new ones.
  // If a user wants to see what's already on the agent, the SkillsTab
  // list above shows it.
  const availableSkills = useMemo(
    () => workspaceSkills.filter((s) => !attachedIds.has(s.id)),
    [workspaceSkills, attachedIds],
  );
  const visibleSkills = useMemo(
    () =>
      labelId === ALL_LABELS
        ? availableSkills
        : availableSkills.filter((s) => (s.labels ?? []).some((l) => l.id === labelId)),
    [availableSkills, labelId],
  );

  const handleOpenChange = (v: boolean) => {
    if (!v) setSelectedIds(new Set());
    onOpenChange(v);
  };

  const handleToggle = (skill: SkillSummary) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(skill.id)) next.delete(skill.id);
      else next.add(skill.id);
      return next;
    });
  };

  const selectAllVisible = () => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      for (const skill of visibleSkills) next.add(skill.id);
      return next;
    });
  };

  const handleConfirm = async () => {
    if (selectedIds.size === 0) return;
    setSaving(true);
    try {
      const newIds = [
        ...agent.skills.map((s) => s.id),
        ...selectedIds,
      ];
      await api.setAgentSkills(agent.id, { skill_ids: newIds });
      qc.invalidateQueries({ queryKey: workspaceKeys.agents(wsId) });
      handleOpenChange(false);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : t(($) => $.tab_body.skills.add_failed_toast));
    } finally {
      setSaving(false);
    }
  };

  const count = selectedIds.size;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle className="text-body">
            {t(($) => $.tab_body.skills.add_dialog_title)}
          </DialogTitle>
        </DialogHeader>

        {skillLabels.length > 0 && (
          <div className="flex items-center gap-2">
            <Select
              value={labelId}
              onValueChange={(v) => v && setLabelId(v)}
              items={[
                { value: ALL_LABELS, label: t(($) => $.tab_body.skills.add_dialog_label_all) },
                ...skillLabels.map((label) => ({ value: label.id, label: label.name })),
              ]}
            >
              <SelectTrigger className="min-w-0 flex-1" aria-label={t(($) => $.tab_body.skills.add_dialog_label_filter)}>
                <ListFilter className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL_LABELS}>
                  {t(($) => $.tab_body.skills.add_dialog_label_all)}
                </SelectItem>
                {skillLabels.map((label) => (
                  <SelectItem key={label.id} value={label.id}>
                    {label.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="shrink-0"
              onClick={selectAllVisible}
              disabled={visibleSkills.length === 0}
            >
              {t(($) => $.tab_body.skills.add_dialog_select_group, {
                count: visibleSkills.length,
              })}
            </Button>
          </div>
        )}

        <SkillPickerList
          skills={visibleSkills}
          selectedIds={selectedIds}
          onToggle={handleToggle}
          loading={isLoading}
          emptyMessage={
            workspaceSkills.length === 0
              ? t(($) => $.tab_body.skills.add_dialog_empty)
              : t(($) => $.tab_body.skills.add_dialog_empty_partial)
          }
        />

        <DialogFooter>
          <Button variant="ghost" onClick={() => handleOpenChange(false)}>
            {t(($) => $.tab_body.skills.add_dialog_cancel)}
          </Button>
          <Button
            onClick={handleConfirm}
            disabled={count === 0 || saving}
          >
            {saving
              ? t(($) => $.tab_body.skills.add_dialog_saving)
              : count > 0
                ? t(($) => $.tab_body.skills.add_dialog_confirm, { count })
                : t(($) => $.tab_body.skills.add_dialog_confirm_default)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
