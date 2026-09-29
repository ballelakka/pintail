<script setup lang="ts">
import { LoaderCircle, Plus, RefreshCw } from '@lucide/vue'
import { toast } from 'vue-sonner'
import { formatDate, messageOf } from '@/lib/format'
import type { AddUpstreamTablesResponse, UpstreamStatus, UpstreamTablesResponse } from '@/types/pintail'

/// Every base table on the source beside where it stands in the catalog.
///
/// The include list and the catalog can drift apart - a table the catalog
/// lost is still selected yet nothing mirrors it - and a source table left
/// out of the selection was invisible from here. This probes the source on
/// open and on refresh, and adds the chosen tables with one snapshot.
const props = defineProps<{ open: boolean; databaseId: string; databaseName: string }>()
const emit = defineEmits<{ 'update:open': [value: boolean]; 'added': [] }>()

const { request } = usePintailApi()
const view = ref<UpstreamTablesResponse | null>(null)
const loadError = ref('')
const probing = ref(false)
const adding = ref(false)
const statusFilter = ref<'all' | UpstreamStatus>('all')
const search = ref('')
const selected = ref<Set<string>>(new Set())

const STATUS_LABELS: Record<UpstreamStatus, string> = {
  'mirrored': 'mirrored',
  'missing': 'missing',
  'not-included': 'not included',
  'dropped-upstream': 'dropped upstream',
}
const STATUS_TONES: Record<UpstreamStatus, string> = {
  'mirrored': 'positive',
  'missing': 'negative',
  'not-included': 'neutral',
  'dropped-upstream': 'warning',
}
const STATUS_HINTS: Record<UpstreamStatus, string> = {
  'mirrored': 'On the source and in the catalog.',
  'missing': 'Selected for replication but absent from the catalog, so nothing mirrors it. Adding it copies it.',
  'not-included': 'On the source, left out by the include or exclude list.',
  'dropped-upstream': 'In the catalog, but the source no longer has it.',
}

/// Only these can be added: a mirrored table already is, and a dropped one
/// has nothing on the source to copy.
function selectable(status: UpstreamStatus) {
  return status === 'missing' || status === 'not-included'
}

const statusCounts = computed(() => {
  const counts = view.value?.counts
  if (!counts) return [] as [UpstreamStatus, number][]
  return ([
    ['missing', counts.missing],
    ['not-included', counts.not_included],
    ['mirrored', counts.mirrored],
    ['dropped-upstream', counts.dropped_upstream],
  ] as [UpstreamStatus, number][]).filter(([, count]) => count > 0)
})

const visibleTables = computed(() => {
  const needle = search.value.trim().toLowerCase()
  return (view.value?.tables ?? []).filter((table) =>
    (statusFilter.value === 'all' || table.status === statusFilter.value)
    && (!needle || table.name.toLowerCase().includes(needle)))
})

const visibleSelectable = computed(() => visibleTables.value.filter((table) => selectable(table.status)))
const allVisibleSelected = computed(() =>
  visibleSelectable.value.length > 0 && visibleSelectable.value.every((table) => selected.value.has(table.name)))

function toggle(name: string, checked: boolean) {
  const next = new Set(selected.value)
  if (checked) next.add(name)
  else next.delete(name)
  selected.value = next
}

function toggleVisible(checked: boolean) {
  const next = new Set(selected.value)
  for (const table of visibleSelectable.value) {
    if (checked) next.add(table.name)
    else next.delete(table.name)
  }
  selected.value = next
}

async function probe() {
  if (probing.value) return
  probing.value = true
  loadError.value = ''
  try {
    view.value = await request<UpstreamTablesResponse>(
      `/databases/${encodeURIComponent(props.databaseId)}/upstream-tables`,
    )
    // A selection naming a table that is no longer selectable would be sent
    // anyway, and refused or silently re-added; it is dropped instead.
    const addable = new Set(view.value.tables.filter((table) => selectable(table.status)).map((table) => table.name))
    selected.value = new Set([...selected.value].filter((name) => addable.has(name)))
    if (statusFilter.value !== 'all' && !statusCounts.value.some(([status]) => status === statusFilter.value)) {
      statusFilter.value = 'all'
    }
  } catch (failure) {
    loadError.value = messageOf(failure)
  } finally {
    probing.value = false
  }
}

async function addSelected() {
  if (adding.value || !selected.value.size) return
  adding.value = true
  try {
    const answer = await request<AddUpstreamTablesResponse>(
      `/databases/${encodeURIComponent(props.databaseId)}/upstream-tables`,
      { method: 'POST', body: JSON.stringify({ tables: [...selected.value] }) },
    )
    const count = selected.value.size
    toast(answer.run_id
      ? `Copying ${count} table${count === 1 ? '' : 's'} as snapshot ${answer.run_id}`
      : `${count} table${count === 1 ? '' : 's'} selected; the copy starts when the running replication job finishes`)
    selected.value = new Set()
    emit('added')
    await probe()
  } catch (failure) {
    toast(`Adding tables failed: ${messageOf(failure)}`)
  } finally {
    adding.value = false
  }
}

watch(
  () => [props.open, props.databaseId] as const,
  ([open]) => {
    if (open) void probe()
  },
  { immediate: true },
)
watch(() => props.databaseId, () => {
  view.value = null
  selected.value = new Set()
})
</script>

<template>
  <Dialog :open="open" @update:open="(value) => emit('update:open', value)">
    <DialogContent class="sm:max-w-4xl">
      <DialogHeader>
        <DialogTitle>Upstream tables</DialogTitle>
        <DialogDescription>
          Every base table on the {{ databaseName }} source, beside where it stands in the mirror.
          <template v-if="view"> Probed {{ formatDate(view.probed_at) }}<template v-if="view.include_all"> · the include list selects every table</template>.</template>
        </DialogDescription>
      </DialogHeader>

      <div class="flex flex-wrap items-center gap-2">
        <Input v-model="search" class="max-w-60" placeholder="Filter by name" aria-label="Filter tables by name" data-testid="upstream-search" />
        <Select v-model="statusFilter">
          <SelectTrigger class="min-w-44" data-testid="upstream-status-filter"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All statuses</SelectItem>
            <SelectItem v-for="[status, count] in statusCounts" :key="status" :value="status">
              {{ STATUS_LABELS[status] }} ({{ count }})
            </SelectItem>
          </SelectContent>
        </Select>
        <Button variant="outline" size="sm" class="ml-auto" :disabled="probing" data-testid="upstream-refresh" @click="probe">
          <LoaderCircle v-if="probing" class="animate-spin" /><RefreshCw v-else /> Refresh
        </Button>
      </div>

      <p v-if="loadError" class="text-destructive text-sm break-words">{{ loadError }}</p>
      <div v-else-if="!view" class="text-muted-foreground grid min-h-40 place-content-center justify-items-center gap-2 text-sm">
        <LoaderCircle class="animate-spin" />
        Probing the source - a large schema takes a while.
      </div>
      <div v-else-if="!visibleTables.length" class="text-muted-foreground grid min-h-40 place-content-center text-sm">No table matches the filter.</div>
      <div v-else class="max-h-[55vh] overflow-auto rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead class="bg-background sticky top-0 z-10 w-10">
                <Checkbox
                  :model-value="allVisibleSelected"
                  :disabled="!visibleSelectable.length"
                  aria-label="Select every addable table shown"
                  @update:model-value="(value) => toggleVisible(value === true)"
                />
              </TableHead>
              <TableHead class="bg-background sticky top-0 z-10">Table</TableHead>
              <TableHead class="bg-background sticky top-0 z-10">Status</TableHead>
              <TableHead class="bg-background sticky top-0 z-10">Source rows</TableHead>
              <TableHead class="bg-background sticky top-0 z-10">Mirror state</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow v-for="table in visibleTables" :key="table.name" :data-testid="`upstream-row-${table.name}`">
              <TableCell>
                <Checkbox
                  v-if="selectable(table.status)"
                  :model-value="selected.has(table.name)"
                  :aria-label="`Select ${table.name}`"
                  @update:model-value="(value) => toggle(table.name, value === true)"
                />
              </TableCell>
              <TableCell>
                <strong class="font-mono text-sm">{{ table.name }}</strong>
                <Badge v-if="table.excluded" variant="outline" class="ml-1.5" title="Named by the exclude list. Adding the table takes it off.">excluded</Badge>
              </TableCell>
              <TableCell>
                <Badge :class="`tone-${STATUS_TONES[table.status]}`" :title="STATUS_HINTS[table.status]">{{ STATUS_LABELS[table.status] }}</Badge>
              </TableCell>
              <TableCell class="font-mono text-sm tabular-nums">
                <template v-if="table.estimated_rows != null">{{ table.rows_are_exact ? '' : '~' }}{{ table.estimated_rows.toLocaleString() }}</template>
                <span v-else class="text-muted-foreground">—</span>
              </TableCell>
              <TableCell class="text-muted-foreground text-sm">{{ table.catalog_state ?? '—' }}</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </div>

      <DialogFooter class="items-center gap-2 sm:justify-between">
        <span class="text-muted-foreground text-xs">
          Adding selects the tables for replication and copies them with a snapshot; the rest of the mirror stays live.
        </span>
        <Button :disabled="adding || !selected.size" data-testid="upstream-add" @click="addSelected">
          <LoaderCircle v-if="adding" class="animate-spin" /><Plus v-else /> Add &amp; snapshot<template v-if="selected.size"> ({{ selected.size }})</template>
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>
