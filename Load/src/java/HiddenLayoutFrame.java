import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.imageio.ImageIO;

/**
 * 
 */

/**
 * @author xingao
 * This class has to be put under default package, in order to access BioLayout
 * classes.
 */
public class HiddenLayoutFrame extends LayoutFrame {

    /**
     * 
     */
    private static final long serialVersionUID = -418177851347137182L;

    private Graph graph;
    private SimpleSaver saver;

    private Field fileField;
    private Field nodesField;
    private Field edgesField;
    private Method saveMethod;

    public HiddenLayoutFrame() throws SecurityException, NoSuchFieldException,
            NoSuchMethodException, IllegalArgumentException,
            IllegalAccessException {
        super();
        
        GlobalEnv.DIRECTIONAL = false;
        
        // get graph
        graph = getGraph();
        
        // get Simple Saver
        Field saverField = LayoutFrame.class.getDeclaredField("m_simpleSaver");
        saverField.setAccessible(true);
        saver = (SimpleSaver) saverField.get(this);
        
        // get handles to the private fields and method
        fileField = SimpleSaver.class.getDeclaredField("m_file");
        fileField.setAccessible(true);

        nodesField = SimpleSaver.class.getDeclaredField("m_vertexIterator");
        nodesField.setAccessible(true);

        edgesField = SimpleSaver.class.getDeclaredField("m_edgeIterator");
        edgesField.setAccessible(true);

        saveMethod = SimpleSaver.class.getDeclaredMethod("saveFile");
        saveMethod.setAccessible(true);
    }

    /*
     * (non-Javadoc)
     * 
     * @see LayoutFrame#prepareProgressBar(int, java.lang.String)
     */
    @Override
    public void prepareProgressBar(int i_max, String i_title) {}

    /*
     * (non-Javadoc)
     * 
     * @see LayoutFrame#incrementProgress(int)
     */
    @Override
    public void incrementProgress(int i_iteration) {}

    /*
     * (non-Javadoc)
     * 
     * @see LayoutFrame#resetProgressBar()
     */
    @Override
    public void resetProgressBar() {}

    /*
     * (non-Javadoc)
     * 
     * @see LayoutFrame#startProgressBar()
     */
    @Override
    public void startProgressBar() {}

    /*
     * (non-Javadoc)
     * 
     * @see java.awt.Window#setVisible(boolean)
     */
    @Override
    public void setVisible(boolean b) {
        // aways hide the window
        super.setVisible(false);
    }

    public void saveFile(File inFile, File outFile, File imgFile)
            throws SecurityException, NoSuchFieldException,
            IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException, IOException {
        // parse the file
        parseFile(inFile);
        
        // set the private fields manually
        fileField.set(saver, outFile);
        nodesField.set(saver, graph.getGraphNodeSet().iterator());
        edgesField.set(saver, graph.getGraphEdges().iterator());

        // save the file
        saveMethod.invoke(saver);

        // save the layout image
        int width = graph.getWidth();
        int height = graph.getHeight();
        BufferedImage image = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_ARGB);
        graph.updateBackImage(image.getGraphics());
        ImageIO.write(image, "PNG", imgFile);
    }
}
