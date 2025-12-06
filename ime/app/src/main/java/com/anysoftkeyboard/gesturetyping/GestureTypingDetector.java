package com.anysoftkeyboard.gesturetyping;

import android.util.SparseArray;
import androidx.annotation.NonNull;
import androidx.annotation.VisibleForTesting;
import com.anysoftkeyboard.base.utils.Logger;
import com.anysoftkeyboard.dictionaries.Dictionary;
import com.anysoftkeyboard.keyboards.Keyboard;
import com.anysoftkeyboard.rx.RxSchedulers;
import com.menny.android.anysoftkeyboard.BuildConfig;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.subjects.ReplaySubject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class GestureTypingDetector {
  private static final String TAG = "ASKGestureTypingDetector";

  private static final double CURVATURE_THRESHOLD = Math.toRadians(170);
  // How many points away from the current point do we use when calculating hasEnoughCurvature?
  private static final int CURVATURE_NEIGHBORHOOD = 1;
  private static final double MAXIMUM_DISTANCE_FILTER = 1_000_000;

  /**
   * Maximum squared distance from gesture start point to accept a word's starting key.
   * Very strict threshold to prevent false matches from distant keys.
   * 150 pixels - only allows nearby keys, rejects significantly different ones.
   */
  private int START_KEY_PROXIMITY_THRESHOLD;

  /**
   * Maximum squared distance from gesture end point to accept a word's ending key.
   * Same threshold as start key (100 pixels) - allows for imprecise gesture endings.
   */
  private int END_KEY_PROXIMITY_THRESHOLD;

  /**
   * Penalty factor for words that start near but not on the exact starting key.
   * Lower value = less penalty, higher value = more penalty.
   */
  private static final double PROXIMITY_PENALTY_FACTOR = 2.0;

  /**
   * Weight factor for direction penalty in distance calculation.
   * This penalizes words whose path direction differs from the gesture direction.
   * Very conservative value to avoid over-penalizing short paths where direction is less meaningful.
   */
  private static final double DIRECTION_PENALTY_WEIGHT = 10.0;

  // How far away do two points of the gesture have to be (distance squared)?
  private final int mMinPointDistanceSquared;

  private final ArrayList<String> mCandidates;
  private final double mFrequencyFactor;

  private final ArrayList<Double> mCandidateWeights;

  private final WorkspaceData mWorkspaceData = new WorkspaceData();

  @NonNull private final Iterable<Keyboard.Key> mKeys;

  @NonNull private SparseArray<Keyboard.Key> mKeysByCharacter = new SparseArray<>();

  @NonNull private List<char[][]> mWords = Collections.emptyList();
  @NonNull private List<int[]> mWordFrequencies = Collections.emptyList();

  @NonNull private Disposable mGeneratingDisposable = Disposables.empty();
  private int mMaxSuggestions;

  public enum LoadingState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private final ReplaySubject<LoadingState> mGenerateStateSubject = ReplaySubject.createWithSize(1);
  private ArrayList<short[]> mWordsCorners = new ArrayList<>();

  public GestureTypingDetector(
      double frequencyFactor,
      int maxSuggestions,
      int minPointDistance,
      @NonNull Iterable<Keyboard.Key> keys) {
    mFrequencyFactor = frequencyFactor;
    mMaxSuggestions = maxSuggestions;
    mCandidates = new ArrayList<>(mMaxSuggestions * 3);
    mCandidateWeights = new ArrayList<>(mMaxSuggestions * 3);
    mMinPointDistanceSquared = minPointDistance * minPointDistance;
    mKeys = keys;

    mGenerateStateSubject.onNext(LoadingState.NOT_LOADED);
  }

  @NonNull
  public Observable<LoadingState> state() {
    return mGenerateStateSubject;
  }

  public void setWords(@NonNull List<char[][]> words, @NonNull List<int[]> wordFrequencies) {
    mWords = words;
    mWordFrequencies = wordFrequencies;

    Logger.d(TAG, "starting generateCorners");
    mGeneratingDisposable.dispose();
    mGenerateStateSubject.onNext(LoadingState.LOADING);
    mGeneratingDisposable =
        generateCornersInBackground(mWords, mWordsCorners, mKeys, mKeysByCharacter, mWorkspaceData)
            .subscribe(mGenerateStateSubject::onNext, mGenerateStateSubject::onError);
  }

  public void destroy() {
    mGeneratingDisposable.dispose();
    mGenerateStateSubject.onNext(LoadingState.NOT_LOADED);
    mGenerateStateSubject.onComplete();
    mWords = Collections.emptyList();
    mWordFrequencies = Collections.emptyList();
    mWordsCorners = new ArrayList<>();
    mKeysByCharacter = new SparseArray<>();
  }

  /**
   * Called when system is under memory pressure. Clears temporary data that can be regenerated.
   * Does NOT clear the word list or pre-computed corners.
   */
  public void trimMemory() {
    Logger.d(TAG, "trimMemory() called, clearing temporary data");

    // Clear candidate results (can be regenerated on next gesture)
    mCandidates.clear();
    mCandidateWeights.clear();

    // Trim ArrayList capacity to actual size
    mCandidates.trimToSize();
    mCandidateWeights.trimToSize();

    // Reset gesture workspace
    mWorkspaceData.reset();

    Logger.d(TAG, "trimMemory() completed");
  }

  private static Single<LoadingState> generateCornersInBackground(
      Iterable<char[][]> words,
      Collection<short[]> wordsCorners,
      Iterable<Keyboard.Key> keys,
      SparseArray<Keyboard.Key> keysByCharacter,
      WorkspaceData workspaceData) {

    workspaceData.reset();
    wordsCorners.clear();
    keysByCharacter.clear();

    return Observable.fromIterable(words)
        .subscribeOn(RxSchedulers.background())
        .map(
            wordsArray ->
                new CornersGenerationData(
                    wordsArray, wordsCorners, keys, keysByCharacter, workspaceData))
        // consider adding here groupBy operator to fan-out the generation of paths
        .flatMap(
            data ->
                Observable.<LoadingState>create(
                    e -> {
                      try {
                        Logger.d(TAG, "generating in BG.");

                        // Fill keysByCharacter map for faster path generation.
                        // This is called for each dictionary,
                        // but we only need to do it once.
                        if (data.mKeysByCharacter.size() == 0) {
                          for (Keyboard.Key key : data.mKeys) {
                            for (int i = 0; i < key.getCodesCount(); ++i) {
                              char c = Character.toLowerCase((char) key.getCodeAtIndex(i, false));
                              data.mKeysByCharacter.put(c, key);
                            }
                          }
                        }

                        for (char[] word : data.mWords) {
                          if (e.isDisposed()) {
                            Logger.d(TAG, "generation cancelled during word processing");
                            return;
                          }
                          short[] path = generatePath(word, data.mKeysByCharacter, data.mWorkspace);
                          data.mWordsCorners.add(path);
                        }

                        if (!e.isDisposed()) {
                          Logger.d(TAG, "generating done");
                          e.onNext(LoadingState.LOADED);
                          e.onComplete();
                        }
                      } catch (OutOfMemoryError oomError) {
                        Logger.e(TAG, oomError, "OOM during corner generation");
                        if (!e.isDisposed()) {
                          e.onError(oomError);
                        }
                      } catch (Exception exception) {
                        Logger.e(TAG, exception, "Error during corner generation");
                        if (!e.isDisposed()) {
                          e.onError(exception);
                        }
                      }
                    }))
        .subscribeOn(RxSchedulers.background())
        .lastOrError()
        .onErrorReturnItem(LoadingState.NOT_LOADED)
        .observeOn(RxSchedulers.mainThread());
  }

  private static short[] generatePath(
      char[] word, SparseArray<Keyboard.Key> keysByCharacter, WorkspaceData workspaceData) {
    workspaceData.reset();
    // word = Normalizer.normalize(word, Normalizer.Form.NFD);
    char lastLetter = '\0';

    // Add points for each key
    for (char c : word) {
      c = Character.toLowerCase(c);
      if (lastLetter == c) continue; // Avoid duplicate letters

      Keyboard.Key keyHit = keysByCharacter.get(c);

      if (keyHit == null) {
        // Try finding the base character instead, e.g., the "e" key instead of "é"
        char baseCharacter = Dictionary.toLowerCase(c);
        keyHit = keysByCharacter.get(baseCharacter);
        if (keyHit == null) {
          Logger.w(TAG, "Key %s not found on keyboard!", c);
          continue;
        }
      }

      lastLetter = c;
      workspaceData.addPoint(Keyboard.Key.getCenterX(keyHit), Keyboard.Key.getCenterY(keyHit));
    }

    return getPathCorners(workspaceData);
  }

  /**
   * Adds a point to the gesture path, if it is meaningful
   *
   * @param x the new pointer X position
   * @param y the new pointer Y position
   * @return squared distance from the previous point. Or 0 if not meaningful.
   */
  public int addPoint(int x, int y) {
    if (mGenerateStateSubject.getValue() != LoadingState.LOADED) return 0;

    int distance = 0;
    if (mWorkspaceData.mCurrentGestureArraySize > 0) {
      int previousIndex = mWorkspaceData.mCurrentGestureArraySize - 1;
      final int dx = mWorkspaceData.mCurrentGestureXs[previousIndex] - x;
      final int dy = mWorkspaceData.mCurrentGestureYs[previousIndex] - y;

      distance = dx * dx + dy * dy;
      if (distance <= mMinPointDistanceSquared) return 0;
    }

    mWorkspaceData.addPoint(x, y);
    return distance;
  }

  public void clearGesture() {
    mWorkspaceData.reset();
  }

  private static short[] getPathCorners(WorkspaceData workspaceData) {
    workspaceData.mMaximaArraySize = 0;
    if (workspaceData.mCurrentGestureArraySize > 0) {
      workspaceData.addMaximaPointOfIndex(0);
    }

    for (int gesturePointIndex = 1;
        gesturePointIndex < workspaceData.mCurrentGestureArraySize - 1;
        gesturePointIndex++) {
      if (hasEnoughCurvature(
          workspaceData.mCurrentGestureXs, workspaceData.mCurrentGestureYs, gesturePointIndex)) {
        workspaceData.addMaximaPointOfIndex(gesturePointIndex);
      }
    }

    if (workspaceData.mCurrentGestureArraySize > 1) {
      workspaceData.addMaximaPointOfIndex(workspaceData.mCurrentGestureArraySize - 1);
    }

    short[] arr = new short[workspaceData.mMaximaArraySize];
    System.arraycopy(workspaceData.mMaximaWorkspace, 0, arr, 0, workspaceData.mMaximaArraySize);
    return arr;
  }

  @VisibleForTesting
  static boolean hasEnoughCurvature(final int[] xs, final int[] ys, final int middlePointIndex) {
    // Calculate the radianValue formed between middlePointIndex, and one point in either
    // direction
    final int startPointIndex = middlePointIndex - CURVATURE_NEIGHBORHOOD;
    final int startX = xs[startPointIndex];
    final int startY = ys[startPointIndex];

    final int endPointIndex = middlePointIndex + CURVATURE_NEIGHBORHOOD;
    final int endX = xs[endPointIndex];
    final int endY = ys[endPointIndex];

    final int middleX = xs[middlePointIndex];
    final int middleY = ys[middlePointIndex];

    final int firstSectionXDiff = startX - middleX;
    final int firstSectionYDiff = startY - middleY;
    final double firstSectionLength =
        Math.sqrt(firstSectionXDiff * firstSectionXDiff + firstSectionYDiff * firstSectionYDiff);

    final int secondSectionXDiff = endX - middleX;
    final int secondSectionYDiff = endY - middleY;
    final double secondSectionLength =
        Math.sqrt(
            secondSectionXDiff * secondSectionXDiff + secondSectionYDiff * secondSectionYDiff);

    final double dotProduct =
        firstSectionXDiff * secondSectionXDiff + firstSectionYDiff * secondSectionYDiff;
    final double radianValue = Math.acos(dotProduct / firstSectionLength / secondSectionLength);

    return radianValue <= CURVATURE_THRESHOLD;
  }

  public ArrayList<String> getCandidates() {
    mCandidates.clear();
    if (mGenerateStateSubject.getValue() != LoadingState.LOADED) {
      return mCandidates;
    }

    final short[] corners = getPathCorners(mWorkspaceData);

    Keyboard.Key startKey = null;
    for (Keyboard.Key k : mKeys) {
      if (k.isInside(corners[0], corners[1])) {
        startKey = k;
        break;
      }
    }

    Keyboard.Key eKey = mKeysByCharacter.get('e');
    START_KEY_PROXIMITY_THRESHOLD = eKey.width * eKey.height;
    END_KEY_PROXIMITY_THRESHOLD = eKey.width + eKey.height;

    mCandidateWeights.clear();
    int dictionaryWordsCornersOffset = 0;
    for (int dictIndex = 0; dictIndex < mWords.size(); dictIndex++) {
      final char[][] words = mWords.get(dictIndex);
      final int[] wordFrequencies = mWordFrequencies.get(dictIndex);
      for (int i = 0; i < words.length; i++) {
        // Check if current word starts with a key close to the gesture start point
        final Keyboard.Key wordStartKey = mKeysByCharacter.get(Dictionary.toLowerCase(words[i][0]));
        if (wordStartKey == null) {
          continue; // Character not found on keyboard
        }

        // Calculate squared distance from gesture start to word's starting key
        final int startDistanceSquared = wordStartKey.squaredDistanceFrom(corners[0], corners[1]);

        // Add a small penalty if the word doesn't start on the exact starting key
        // This biases towards words that start on the gesture starting key
        double startkeyPenalty = 0;
        if (startDistanceSquared > START_KEY_PROXIMITY_THRESHOLD) {
            startkeyPenalty = Math.sqrt(startDistanceSquared - START_KEY_PROXIMITY_THRESHOLD ) * PROXIMITY_PENALTY_FACTOR;
        }

        // Improvement 2: Calculate end-key distance penalty
        // Instead of hard rejection, we add a penalty for words that end far from gesture end
        // This allows some flexibility while still preferring words that end near the gesture end
        double endKeyPenalty = 0;
        final char lastChar = words[i][words[i].length - 1];
        final Keyboard.Key wordEndKey = mKeysByCharacter.get(Dictionary.toLowerCase(lastChar));
        if (wordEndKey != null) {
          // Calculate squared distance from gesture end to word's ending key
          final int endCornerX = corners[corners.length - 2];
          final int endCornerY = corners[corners.length - 1];
          final int endDistanceSquared = wordEndKey.squaredDistanceFrom(endCornerX, endCornerY);

          // Apply penalty proportional to end-key distance (similar to start-key proximity)
          // Penalty increases for keys further away, encouraging words that end near gesture end
          if (endDistanceSquared > END_KEY_PROXIMITY_THRESHOLD) {
            endKeyPenalty = Math.sqrt(endDistanceSquared - END_KEY_PROXIMITY_THRESHOLD) * PROXIMITY_PENALTY_FACTOR;
          }
        }

        final double distanceFromCurve =
            calculateDistanceBetweenUserPathAndWord(
                corners, mWordsCorners.get(i + dictionaryWordsCornersOffset));
        if (distanceFromCurve > MAXIMUM_DISTANCE_FILTER) {
          continue;
        }

        // TODO: convert wordFrequencies to a double[] in the loading phase.
        double frequencyAdvantage = mFrequencyFactor * ((double) wordFrequencies[i]);
        final double revisedDistanceFromCurve = distanceFromCurve - frequencyAdvantage;

        final double finalWeight = revisedDistanceFromCurve + startkeyPenalty + endKeyPenalty;

        int candidateDistanceSortedIndex = 0;
        while (candidateDistanceSortedIndex < mCandidateWeights.size()
            && mCandidateWeights.get(candidateDistanceSortedIndex) <= finalWeight) {
          candidateDistanceSortedIndex++;
        }

        if (candidateDistanceSortedIndex < mMaxSuggestions) {
          mCandidateWeights.add(candidateDistanceSortedIndex, finalWeight);
          mCandidates.add(candidateDistanceSortedIndex, new String(words[i]) +
                  String.format(Locale.getDefault(),
                          "(dist=%.0f:startpen=%.0f:endpen=%.0f:freqadv=%.0f:final=%.0f)",
                          distanceFromCurve, startkeyPenalty, endKeyPenalty, frequencyAdvantage, finalWeight));
          if (mCandidateWeights.size() > mMaxSuggestions) {
            mCandidateWeights.remove(mMaxSuggestions);
            mCandidates.remove(mMaxSuggestions);
          }
        }
      }

      dictionaryWordsCornersOffset += words.length;
    }

    return mCandidates;
  }

  private static double calculateDistanceBetweenUserPathAndWord(
      short[] actualUserPath, short[] generatedWordPath) {
    // Debugging is still needed, but at least ASK won't crash this way
    if (actualUserPath.length < 2 || generatedWordPath.length == 0) {
      Logger.w(
          TAG,
          "calculateDistanceBetweenUserPathAndWord: actualUserPath = \"%s\","
              + " generatedWordPath = \"%s\"",
          actualUserPath,
          generatedWordPath);
      Logger.w(TAG, "Some strings are too short; will return maximum distance.");
      return Double.MAX_VALUE;
    }

    // Keep original hard rejection: words cannot be longer than the gesture
    // This is essential for filtering out false matches from completely different words
    if (generatedWordPath.length > actualUserPath.length) return Double.MAX_VALUE;

    double cumulativeDistance = 0;
    int generatedWordCornerIndex = 0;

    for (int userPathIndex = 0; userPathIndex < actualUserPath.length; userPathIndex += 2) {
      final int ux = actualUserPath[userPathIndex];
      final int uy = actualUserPath[userPathIndex + 1];
      double distanceToGeneratedCorner =
          distSquared(
              ux,
              uy,
              generatedWordPath[generatedWordCornerIndex],
              generatedWordPath[generatedWordCornerIndex + 1]);

      if (generatedWordCornerIndex < generatedWordPath.length - 2) {
        // maybe this new point is closer to the next corner?
        // we only need to check one point ahead since the generated path little corners.
        final double distanceToNextGeneratedCorner =
                distSquared(
                ux,
                uy,
                generatedWordPath[generatedWordCornerIndex + 2],
                generatedWordPath[generatedWordCornerIndex + 3]);
        if (distanceToNextGeneratedCorner < distanceToGeneratedCorner) {
          generatedWordCornerIndex += 2;
          // We don't want to fully disregard the distance to this corner, so we keep half of it.
          // This way we add a penalty for words that match a gesture path with a redundant corner.
          distanceToGeneratedCorner = distanceToGeneratedCorner / 1.5 + distanceToNextGeneratedCorner;
        }
      }

      // Improvement 3: Add direction penalty to penalize words with mismatched path direction
      final double directionPenalty =
          calculateDirectionPenalty(
              actualUserPath, userPathIndex, generatedWordPath, generatedWordCornerIndex);

      cumulativeDistance += distanceToGeneratedCorner + directionPenalty;
    }

    // we finished the user-path, but for this word there could still be additional
    // generated-path corners.
    // we'll need to those too.
    for (int ux = actualUserPath[actualUserPath.length - 2],
            uy = actualUserPath[actualUserPath.length - 1];
        generatedWordCornerIndex < generatedWordPath.length;
        generatedWordCornerIndex += 2) {
      cumulativeDistance +=
          distSquared(
              ux,
              uy,
              generatedWordPath[generatedWordCornerIndex],
              generatedWordPath[generatedWordCornerIndex + 1]);
    }

    return cumulativeDistance;
  }

  private static double distSquared(double x1, double y1, double x2, double y2) {
    return (x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1);
  }

  private static double dist(double x1, double y1, double x2, double y2) {
    return Math.sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
  }

  /**
   * Calculates direction penalty based on cosine similarity between gesture and word path
   * directions. Returns 0 if directions align well (similar direction vectors), returns higher
   * values if directions differ (opposite or perpendicular directions).
   *
   * @param actualPath the user's gesture path (x,y coordinates as shorts)
   * @param userIdx current index in user path (must be >= 2 to have previous point)
   * @param wordPath the pre-computed word path (x,y coordinates as shorts)
   * @param wordIdx current index in word path (must be >= 2 to have previous point)
   * @return penalty value (0 for matching directions, higher for mismatched directions)
   */
  private static double calculateDirectionPenalty(
      short[] actualPath, int userIdx, short[] wordPath, int wordIdx) {

    // Need at least 2 corners (4 coordinates) to calculate direction
    if (userIdx < 2 || wordIdx < 2) {
      return 0;
    }

    // Calculate user gesture direction vector (from previous corner to current corner)
    final double userDx = actualPath[userIdx] - actualPath[userIdx - 2];
    final double userDy = actualPath[userIdx + 1] - actualPath[userIdx - 1];

    // Calculate word path direction vector (from previous corner to current corner)
    final double wordDx = wordPath[wordIdx] - wordPath[wordIdx - 2];
    final double wordDy = wordPath[wordIdx + 1] - wordPath[wordIdx - 1];

    // Calculate magnitudes (lengths) of direction vectors
    final double userLength = Math.sqrt(userDx * userDx + userDy * userDy);
    final double wordLength = Math.sqrt(wordDx * wordDx + wordDy * wordDy);

    // Avoid division by zero for zero-length segments
    if (userLength < 0.1 || wordLength < 0.1) {
      return 0;
    }

    // Calculate dot product of direction vectors
    final double dotProduct = userDx * wordDx + userDy * wordDy;

    // Calculate cosine similarity: 1.0 = same direction, 0 = perpendicular, -1.0 = opposite
    final double cosineSimilarity = dotProduct / (userLength * wordLength);

    // Convert to penalty: 0 for same direction, increases as directions differ
    // (1 - cosineSimilarity) ranges from 0 (same) to 2 (opposite)
    final double directionDifference = 1.0 - cosineSimilarity;

    return directionDifference * DIRECTION_PENALTY_WEIGHT;
  }

  private static class WorkspaceData {
    static final int MAX_GESTURE_LENGTH = 1024;
    private int mCurrentGestureArraySize = 0;
    private final int[] mCurrentGestureXs = new int[MAX_GESTURE_LENGTH];
    private final int[] mCurrentGestureYs = new int[MAX_GESTURE_LENGTH];

    private int mMaximaArraySize = 0;
    private final short[] mMaximaWorkspace = new short[4 * MAX_GESTURE_LENGTH];

    void reset() {
      mCurrentGestureArraySize = 0;
      mMaximaArraySize = 0;
    }

    void addPoint(int x, int y) {
      if (MAX_GESTURE_LENGTH == mCurrentGestureArraySize) {
        if (BuildConfig.TESTING_BUILD) {
          Logger.w(TAG, "Discarding gesture");
        }
        return;
      }

      mCurrentGestureXs[mCurrentGestureArraySize] = x;
      mCurrentGestureYs[mCurrentGestureArraySize] = y;
      mCurrentGestureArraySize++;
    }

    void addMaximaPointOfIndex(int gesturePointIndex) {
      mMaximaWorkspace[mMaximaArraySize] = (short) mCurrentGestureXs[gesturePointIndex];
      mMaximaArraySize++;
      mMaximaWorkspace[mMaximaArraySize] = (short) mCurrentGestureYs[gesturePointIndex];
      mMaximaArraySize++;
    }
  }

  private static class CornersGenerationData {
    private final char[][] mWords;
    private final Collection<short[]> mWordsCorners;
    private final Iterable<Keyboard.Key> mKeys;
    private final SparseArray<Keyboard.Key> mKeysByCharacter;
    private final WorkspaceData mWorkspace;

    CornersGenerationData(
        char[][] words,
        Collection<short[]> wordsCorners,
        Iterable<Keyboard.Key> keys,
        SparseArray<Keyboard.Key> keysByCharacter,
        WorkspaceData workspace) {
      mWords = words;
      mWordsCorners = wordsCorners;
      mKeys = keys;
      mKeysByCharacter = keysByCharacter;
      mWorkspace = workspace;
    }
  }
}
