package io.nexus.actor.scene.aoi;

/**
 * 世界地图配置，定义地图尺寸和区域划分规则。
 * 由上层从 WorldMap.json 解析后注入。
 */
public class WorldMapConfig {

    private final int mapWidth;
    private final int mapHeight;
    private final int areaWidth;
    private final int areaHeight;
    private final int areasPerRow;
    private final int areasPerCol;
    private final int totalAreas;

    public WorldMapConfig(int mapWidth, int mapHeight, int areaWidth, int areaHeight) {
        this.mapWidth = mapWidth;
        this.mapHeight = mapHeight;
        this.areaWidth = areaWidth;
        this.areaHeight = areaHeight;
        this.areasPerRow = (int) Math.ceil((double) mapWidth / areaWidth);
        this.areasPerCol = (int) Math.ceil((double) mapHeight / areaHeight);
        this.totalAreas = areasPerRow * areasPerCol;
    }

    /**
     * 默认配置：1000x1000 地图，75x75 区域
     */
    public static WorldMapConfig defaultConfig() {
        return new WorldMapConfig(1000, 1000, 75, 75);
    }

    public int getMapWidth() { return mapWidth; }
    public int getMapHeight() { return mapHeight; }
    public int getAreaWidth() { return areaWidth; }
    public int getAreaHeight() { return areaHeight; }
    public int getAreasPerRow() { return areasPerRow; }
    public int getAreasPerCol() { return areasPerCol; }
    public int getTotalAreas() { return totalAreas; }

    /**
     * 根据格子坐标计算所属区域 ID（从 1 开始）
     */
    public int gridToAreaId(int gridX, int gridY) {
        int col = gridX / areaWidth;
        int row = gridY / areaHeight;
        return row * areasPerRow + col + 1;
    }

    /**
     * 根据浮点坐标计算所属区域 ID
     */
    public int posToAreaId(float x, float y) {
        return gridToAreaId((int) x, (int) y);
    }

    /**
     * 获取区域的左下角坐标
     */
    public int[] getAreaOrigin(int areaId) {
        int idx = areaId - 1;
        int col = idx % areasPerRow;
        int row = idx / areasPerRow;
        return new int[]{col * areaWidth, row * areaHeight};
    }

    /**
     * 获取区域的覆盖范围 [xMin, xMax, yMin, yMax]
     */
    public int[] getAreaBounds(int areaId) {
        int[] origin = getAreaOrigin(areaId);
        return new int[]{
                origin[0],
                Math.min(origin[0] + areaWidth - 1, mapWidth - 1),
                origin[1],
                Math.min(origin[1] + areaHeight - 1, mapHeight - 1)
        };
    }

    /**
     * 判断区域 ID 是否有效
     */
    public boolean isValidAreaId(int areaId) {
        return areaId >= 1 && areaId <= totalAreas;
    }

    /**
     * 判断坐标是否在地图范围内
     */
    public boolean isInBounds(float x, float y) {
        return x >= 0 && x < mapWidth && y >= 0 && y < mapHeight;
    }
}
