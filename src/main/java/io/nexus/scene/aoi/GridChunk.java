package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;

import java.util.*;

/**
 * Grid 分组（Chunk），介于 Area 和 Grid 之间的订阅粒度。
 *
 * 每个 Area(75x75) 按 chunkSize 细分为多个 Chunk。
 * 例如 chunkSize=15 时，每个 Area 有 5x5=25 个 Chunk。
 *
 * Stream key 格式：scene:change:{serverId}:{areaId}:{chunkId}
 * Slave 只订阅视口覆盖的 Chunk，而非整个 Area。
 *
 * 带宽节省：视口 20x20 格子，覆盖 ~4-6 个 Chunk（而非整个 Area 的 25 个 Chunk）。
 */
public class GridChunk {

    private final int chunkSize;
    private final int chunksPerAreaRow;
    private final int chunksPerAreaCol;
    private final WorldMapConfig mapConfig;

    public GridChunk(WorldMapConfig mapConfig, int chunkSize) {
        this.mapConfig = mapConfig;
        this.chunkSize = chunkSize;
        this.chunksPerAreaRow = (int) Math.ceil((double) mapConfig.getAreaWidth() / chunkSize);
        this.chunksPerAreaCol = (int) Math.ceil((double) mapConfig.getAreaHeight() / chunkSize);
    }

    /**
     * 默认 chunk 大小 15：每个 75x75 Area 分为 5x5=25 个 Chunk
     */
    public GridChunk(WorldMapConfig mapConfig) {
        this(mapConfig, 15);
    }

    /**
     * 根据格子坐标计算 ChunkId（Area 内局部 ID）
     */
    public int gridToLocalChunkId(int gridX, int gridY, int areaId) {
        int[] origin = mapConfig.getAreaOrigin(areaId);
        int localX = gridX - origin[0];
        int localY = gridY - origin[1];
        int chunkCol = localX / chunkSize;
        int chunkRow = localY / chunkSize;
        return chunkRow * chunksPerAreaRow + chunkCol;
    }

    /**
     * 根据格子坐标计算全局 ChunkKey（用于 Stream key）
     */
    public ChunkKey gridToChunkKey(int gridX, int gridY) {
        int areaId = mapConfig.gridToAreaId(gridX, gridY);
        int localChunkId = gridToLocalChunkId(gridX, gridY, areaId);
        return new ChunkKey(areaId, localChunkId);
    }

    /**
     * 根据 GridId 计算 ChunkKey
     */
    public ChunkKey gridToChunkKey(GridId grid) {
        return gridToChunkKey(grid.x(), grid.y());
    }

    /**
     * 计算视口覆盖的所有 ChunkKey
     */
    public Set<ChunkKey> viewPortToChunkKeys(ViewPort viewPort) {
        ViewPort clamped = viewPort.clamp(mapConfig.getMapWidth(), mapConfig.getMapHeight());
        Set<ChunkKey> chunks = new HashSet<>();

        int minX = clamped.lb().x();
        int maxX = clamped.rt().x();
        int minY = clamped.lb().y();
        int maxY = clamped.rt().y();

        // 按 chunkSize 步进，避免逐格子遍历
        for (int x = minX; x <= maxX; x += chunkSize) {
            for (int y = minY; y <= maxY; y += chunkSize) {
                chunks.add(gridToChunkKey(Math.min(x, maxX), Math.min(y, maxY)));
            }
        }
        // 补充边界 chunk
        for (int x = minX; x <= maxX; x += chunkSize) {
            chunks.add(gridToChunkKey(Math.min(x, maxX), maxY));
        }
        for (int y = minY; y <= maxY; y += chunkSize) {
            chunks.add(gridToChunkKey(maxX, Math.min(y, maxY)));
        }
        chunks.add(gridToChunkKey(maxX, maxY));

        return chunks;
    }

    /**
     * 计算视口移动后的 Chunk 订阅变更
     */
    public ChunkDiff calcChunkDiff(ViewPort oldView, ViewPort newView) {
        Set<ChunkKey> oldChunks = viewPortToChunkKeys(oldView);
        Set<ChunkKey> newChunks = viewPortToChunkKeys(newView);

        Set<ChunkKey> added = new HashSet<>(newChunks);
        added.removeAll(oldChunks);

        Set<ChunkKey> removed = new HashSet<>(oldChunks);
        removed.removeAll(newChunks);

        return new ChunkDiff(added, removed, newChunks);
    }

    /**
     * 生成 Stream key
     */
    public String toStreamKey(int serverId, ChunkKey chunkKey) {
        return "scene:change:" + serverId + ":" + chunkKey.areaId() + ":" + chunkKey.localChunkId();
    }

    /**
     * 获取一个 Chunk 内包含的所有 GridId
     */
    public Set<GridId> getChunkGrids(ChunkKey chunkKey) {
        int[] origin = mapConfig.getAreaOrigin(chunkKey.areaId());
        int chunkCol = chunkKey.localChunkId() % chunksPerAreaRow;
        int chunkRow = chunkKey.localChunkId() / chunksPerAreaRow;

        int startX = origin[0] + chunkCol * chunkSize;
        int startY = origin[1] + chunkRow * chunkSize;
        int endX = Math.min(startX + chunkSize - 1, mapConfig.getMapWidth() - 1);
        int endY = Math.min(startY + chunkSize - 1, mapConfig.getMapHeight() - 1);

        Set<GridId> grids = new HashSet<>();
        for (int x = startX; x <= endX; x++) {
            for (int y = startY; y <= endY; y++) {
                grids.add(new GridId(x, y));
            }
        }
        return grids;
    }

    public int getChunkSize() { return chunkSize; }
    public int getChunksPerAreaRow() { return chunksPerAreaRow; }
    public int getChunksPerAreaCol() { return chunksPerAreaCol; }
    public int getTotalChunksPerArea() { return chunksPerAreaRow * chunksPerAreaCol; }

    /**
     * Chunk 唯一标识
     */
    public record ChunkKey(int areaId, int localChunkId) {
        public String toKey() {
            return areaId + ":" + localChunkId;
        }
    }

    /**
     * Chunk 订阅变更
     */
    public record ChunkDiff(Set<ChunkKey> added, Set<ChunkKey> removed, Set<ChunkKey> current) {
        public boolean hasChanges() {
            return !added.isEmpty() || !removed.isEmpty();
        }
    }
}
