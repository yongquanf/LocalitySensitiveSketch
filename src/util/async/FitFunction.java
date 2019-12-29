package util.async;

/**
 * we use fitFunction in simplex Downhill computation as input functions
 * 
 * @author wang
 * 
 */
public interface FitFunction {

	/**
	 * 
	 * @param rawCoordinates
	 *            , Note: we don't use the 0th element
	 * @param totalCoordinateElements
	 * @param HelperDis
	 * @return
	 */
	float fitFunction(float[] rawCoordinates, int totalCoordinateElements,
			float[] HelperDis);
}
