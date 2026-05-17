package io.nexus.actor.scene.aoi;

import io.nexus.actor.view.GridId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AoiCalculatorTest {

    private AoiCalculator calculator;

    @BeforeEach
    void setUp() {
        calculator = new AoiCalculator(WorldMapConfig.defaultConfig());
    }

    @Test
    void calcSubscribedGrids_singleGrid() {
        ViewPort vp = new ViewPort(new GridId(0, 0), new GridId(0, 0));
        Map<Integer, Set<GridId>> result = calculator.calcSubscribedGrids(vp);

        assertEquals(1, result.size());
        assertTrue(result.get(0).contains(new GridId(0, 0)));
    }

    @Test
    void calcSubscribedGrids_withinSingleArea() {
        ViewPort vp = new ViewPort(new GridId(0, 0), new GridId(5, 5));
        Map<Integer, Set<GridId>> result = calculator.calcSubscribedGrids(vp);

        assertEquals(1, result.size());
        assertEquals(36, result.get(0).size());
    }

    @Test
    void calcSubscribedGrids_spansTwoAreas() {
        ViewPort vp = new ViewPort(new GridId(70, 0), new GridId(80, 5));
        Map<Integer, Set<GridId>> result = calculator.calcSubscribedGrids(vp);

        assertEquals(2, result.size());
        assertTrue(result.containsKey(0));
        assertTrue(result.containsKey(1));
    }

    @Test
    void calcSubscribedGrids_clampsToMapBounds() {
        ViewPort vp = new ViewPort(new GridId(-5, -5), new GridId(5, 5));
        Map<Integer, Set<GridId>> result = calculator.calcSubscribedGrids(vp);

        for (Set<GridId> grids : result.values()) {
            for (GridId g : grids) {
                assertTrue(g.x() >= 0);
                assertTrue(g.y() >= 0);
            }
        }
    }

    @Test
    void calcSubscribedGrids_clampsUpperBound() {
        ViewPort vp = new ViewPort(new GridId(995, 995), new GridId(1005, 1005));
        Map<Integer, Set<GridId>> result = calculator.calcSubscribedGrids(vp);

        for (Set<GridId> grids : result.values()) {
            for (GridId g : grids) {
                assertTrue(g.x() <= 999);
                assertTrue(g.y() <= 999);
            }
        }
    }

    @Test
    void calcViewDiff_noOverlap() {
        ViewPort oldView = new ViewPort(new GridId(0, 0), new GridId(5, 5));
        ViewPort newView = new ViewPort(new GridId(100, 100), new GridId(105, 105));

        AoiDiff diff = calculator.calcViewDiff(oldView, newView);

        assertTrue(diff.hasChanges());
        assertFalse(diff.added().isEmpty());
        assertFalse(diff.removed().isEmpty());
    }

    @Test
    void calcViewDiff_sameView() {
        ViewPort view = new ViewPort(new GridId(10, 10), new GridId(20, 20));

        AoiDiff diff = calculator.calcViewDiff(view, view);

        assertFalse(diff.hasChanges());
        assertTrue(diff.added().isEmpty());
        assertTrue(diff.removed().isEmpty());
    }

    @Test
    void calcViewDiff_partialOverlap() {
        ViewPort oldView = new ViewPort(new GridId(0, 0), new GridId(10, 10));
        ViewPort newView = new ViewPort(new GridId(5, 5), new GridId(15, 15));

        AoiDiff diff = calculator.calcViewDiff(oldView, newView);

        assertTrue(diff.hasChanges());
        assertFalse(diff.allAddedGrids().isEmpty());
        assertFalse(diff.allRemovedGrids().isEmpty());
    }

    @Test
    void calcViewDiff_newAreaIds() {
        ViewPort oldView = new ViewPort(new GridId(0, 0), new GridId(5, 5));
        ViewPort newView = new ViewPort(new GridId(200, 200), new GridId(205, 205));

        AoiDiff diff = calculator.calcViewDiff(oldView, newView);

        assertFalse(diff.newAreaIds().isEmpty());
        assertFalse(diff.removedAreaIds().isEmpty());
    }

    @Test
    void calcMarchPathGrids_singlePoint() {
        List<GridId> path = List.of(new GridId(5, 5));
        Map<Integer, Set<GridId>> result = calculator.calcMarchPathGrids(path);

        assertEquals(1, result.size());
        assertTrue(result.get(0).contains(new GridId(5, 5)));
    }

    @Test
    void calcMarchPathGrids_straightLine() {
        List<GridId> path = List.of(new GridId(0, 0), new GridId(10, 0));
        Map<Integer, Set<GridId>> result = calculator.calcMarchPathGrids(path);

        int totalGrids = result.values().stream().mapToInt(Set::size).sum();
        assertEquals(11, totalGrids);
    }

    @Test
    void calcMarchPathGrids_crossesAreaBoundary() {
        List<GridId> path = List.of(new GridId(70, 0), new GridId(80, 0));
        Map<Integer, Set<GridId>> result = calculator.calcMarchPathGrids(path);

        assertTrue(result.size() >= 2);
    }

    @Test
    void calcDistance_samePoint() {
        assertEquals(0f, calculator.calcDistance(new GridId(5, 5), new GridId(5, 5)));
    }

    @Test
    void calcDistance_horizontal() {
        assertEquals(10f, calculator.calcDistance(new GridId(0, 0), new GridId(10, 0)));
    }

    @Test
    void calcDistance_diagonal() {
        float dist = calculator.calcDistance(new GridId(0, 0), new GridId(3, 4));
        assertEquals(5f, dist, 0.001f);
    }

    @Test
    void calcPathLength_twoPoints() {
        List<GridId> path = List.of(new GridId(0, 0), new GridId(3, 4));
        assertEquals(5f, calculator.calcPathLength(path), 0.001f);
    }

    @Test
    void calcPathLength_emptyPath() {
        assertEquals(0f, calculator.calcPathLength(List.of()));
    }

    @Test
    void getGridsInRadius_zeroRadius() {
        Set<GridId> grids = calculator.getGridsInRadius(new GridId(50, 50), 0);
        assertEquals(1, grids.size());
        assertTrue(grids.contains(new GridId(50, 50)));
    }

    @Test
    void getGridsInRadius_circularShape() {
        Set<GridId> grids = calculator.getGridsInRadius(new GridId(50, 50), 3);
        assertTrue(grids.contains(new GridId(50, 50)));
        assertTrue(grids.contains(new GridId(53, 50)));
        assertFalse(grids.contains(new GridId(53, 53)));
    }

    @Test
    void getGridsInRadius_clampsToMapBounds() {
        Set<GridId> grids = calculator.getGridsInRadius(new GridId(0, 0), 5);
        for (GridId g : grids) {
            assertTrue(g.x() >= 0);
            assertTrue(g.y() >= 0);
        }
    }

    @Test
    void getAreaId_firstArea() {
        assertEquals(0, calculator.getAreaId(new GridId(0, 0)));
        assertEquals(0, calculator.getAreaId(new GridId(74, 74)));
    }

    @Test
    void getAreaId_secondArea() {
        assertEquals(1, calculator.getAreaId(new GridId(75, 0)));
    }
}
