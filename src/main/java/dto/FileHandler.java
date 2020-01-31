package dto;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class FileHandler {
    private String inputFile;
    private String outputFile;
    private AtomicInteger numOfSentReviews = new AtomicInteger(0);
    private AtomicInteger numOfHandledReviews = new AtomicInteger(0);
    private Boolean isFinishedSendingReview = false;
    private BufferedWriter outputBuffer;


    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public AtomicInteger getNumOfSentReviews() {
        return numOfSentReviews;
    }

    public AtomicInteger getNumOfHandledReviews() {
        return numOfHandledReviews;
    }

    public synchronized Boolean getFinishedSendingReview() {
        return isFinishedSendingReview;
    }

    public synchronized void setFinishedSendingReview(Boolean finishedSendingReview) {
        isFinishedSendingReview = finishedSendingReview;
    }

    public FileHandler(String inputFile, String outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        try {
            this.outputBuffer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public BufferedWriter getOutputBuffer() {
        return outputBuffer;
    }

    public void setOutputBuffer(BufferedWriter outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    public void incrementHandledReviews() {
        this.numOfHandledReviews.incrementAndGet();
    }
    public void incrementSentReviews() {
        this.numOfSentReviews.incrementAndGet();
    }
}
