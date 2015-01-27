
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Property;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.CustomField;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.Upload;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Field} for editing {@link Property}s with {@code byte[]} array values.
 * The {@code byte[]} value is updated using a file upload.
 */
@SuppressWarnings("serial")
public class BlobField extends CustomField<byte[]> implements Upload.Receiver,
  Upload.StartedListener, Upload.ProgressListener, Upload.SucceededListener, Upload.FailedListener {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Label descriptionLabel = new Label();
    private final HorizontalLayout layout = new HorizontalLayout();
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final ProgressBar progressBar = new ProgressBar();
    private final Upload upload = new Upload();

    /**
     * Constructor.
     */
    public BlobField() {
        this.addValueChangeListener(new Property.ValueChangeListener() {
            @Override
            public void valueChange(Property.ValueChangeEvent event) {
                BlobField.this.updateDisplay();
            }
        });
        this.addReadOnlyStatusChangeListener(new Property.ReadOnlyStatusChangeListener() {
            @Override
            public void readOnlyStatusChange(Property.ReadOnlyStatusChangeEvent event) {
                BlobField.this.updateDisplay();
            }
        });
    }

    @Override
    public void attach() {
        super.attach();
        this.updateDisplay();
    }

    // Cancel any upload in progress if we are detached
    @Override
    public void detach() {
        super.detach();
        this.upload.interruptUpload();
    }

    /**
     * Get the {@link Upload} associated with this instance.
     */
    public Upload getUpload() {
        return this.upload;
    }

    /**
     * Get the description {@link Label} associated with this instance.
     */
    public Label getDescriptionLabel() {
        return this.descriptionLabel;
    }

// CustomField

    @Override
    public Class<byte[]> getType() {
        return byte[].class;
    }

    @Override
    protected Component initContent() {

        // Initialize layout
        this.layout.setMargin(false);
        this.layout.setSpacing(true);

        // Add description
        this.descriptionLabel.setSizeUndefined();
        this.layout.addComponent(this.descriptionLabel);
        this.layout.setComponentAlignment(this.descriptionLabel, Alignment.MIDDLE_LEFT);
        this.layout.addComponent(new Label("\u00a0\u00a0"));

        // Add upload
        this.upload.setReceiver(this);
        this.upload.addStartedListener(this);
        this.upload.addProgressListener(this);
        this.upload.addSucceededListener(this);
        this.upload.addFailedListener(this);
        this.upload.setImmediate(true);
        this.layout.addComponent(this.upload);

        // Add progress bar
        this.progressBar.setIndeterminate(false);
        this.progressBar.setVisible(false);
        this.layout.addComponent(this.progressBar);
        this.layout.setComponentAlignment(this.progressBar, Alignment.MIDDLE_LEFT);
        this.layout.setExpandRatio(this.progressBar, 1.0f);

        // Done
        return layout;
    }

    @Override
    protected void setInternalValue(byte[] value) {
        super.setInternalValue(value);
        this.updateDisplay();
    }

// Upload.Receiver

    @Override
    public OutputStream receiveUpload(String filename, String mimeType) {
        return this.buffer;
    }

// Upload.StartedListener

    @Override
    public void uploadStarted(Upload.StartedEvent event) {
        this.log.info("started upload of file `" + event.getFilename() + "'");
        this.progressBar.setValue(0.0f);
        this.progressBar.setVisible(true);
        this.updateDisplay();
    }

// Upload.ProgressListener

    @Override
    public void updateProgress(long readBytes, long contentLength) {
        float fraction = (float)readBytes / (float)contentLength;
        fraction = Math.max(fraction, 0.0f);
        fraction = Math.min(fraction, 1.0f);
        fraction = (int)(fraction * 64.0f) / 64.0f;                 // quantize fraction to avoid zillions of PUSH updates
        this.progressBar.setValue(fraction);
    }

// Upload.SucceededListener

    @Override
    public void uploadSucceeded(Upload.SucceededEvent event) {
        final byte[] data = this.buffer.toByteArray();
        this.log.info("finished upload of file `" + event.getFilename() + "' (" + data.length + " bytes)");
        this.buffer.reset();
        this.setValue(data);
        this.progressBar.setVisible(false);
        this.updateDisplay();
    }

// Upload.FailedListener

    @Override
    public void uploadFailed(Upload.FailedEvent event) {
        this.log.info("failed upload of file `" + event.getFilename() + "': " + event.getReason());
        this.progressBar.setVisible(false);
        this.updateDisplay();
        if (this.getUI() != null) {
            final Notification notification = new Notification("Upload failed",
              "" + event.getReason(), Notification.Type.ERROR_MESSAGE);
            notification.setDelayMsec(3000);
            notification.show(this.getUI().getPage());
        }
    }

// Internal Methods

    /**
     * Update the description label to reflect the current property value and read-only status.
     */
    protected void updateDisplay() {
        final byte[] value = this.getValue();
        this.descriptionLabel.setValue(value != null ? value.length + " bytes" : "Null");
        this.upload.setEnabled(!this.isReadOnly());
    }
}

