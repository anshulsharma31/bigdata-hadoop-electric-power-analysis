/*
* Copyright 2017 AMSTECH INCORPORATION PRIVATE LIMITED.
* (www.amstechinc.com)
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package amstech.demo.electric.power.analysis.frame;

import amstech.demo.electric.power.analysis.mr.PeriodicAnalysisDriver;
import amstech.demo.electric.power.analysis.util.HDFSHelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.util.StringUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

public class ReportDialog extends javax.swing.JDialog {

    /**
     * Creates new form ReportDialog
     */
    public ReportDialog(java.awt.Frame parent, boolean modal) {
        super(parent, modal);
        initComponents();
    }

    String hdfsURL;
    String attributeName;
    int attributeIndex;
    String periodType;

    public ReportDialog(java.awt.Frame parent, boolean modal, String hdfsURL, String attributeName, int attributeIndex, String periodType) throws Exception {
        super(parent, modal);
        initComponents();
        this.hdfsURL = hdfsURL;
        this.attributeName = attributeName;
        this.attributeIndex = attributeIndex;
        this.periodType = periodType;
        JFreeChart lineChart = ChartFactory.createLineChart(
                attributeName + " " + periodType + " analysis",
                periodType + " analysis", attributeName,
                createDataset(),
                PlotOrientation.VERTICAL,
                true, true, false);

        ChartPanel chartPanel = new ChartPanel(lineChart);
        chartPanel.setPreferredSize(new java.awt.Dimension(600, 400));
        setContentPane(chartPanel);

    }

    private DefaultCategoryDataset createDataset()
            throws Exception {
        String outlier = String.valueOf(PeriodicAnalysisDriver.OUTLIER);
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        List<String> hdfsFileData = HDFSHelper.hdfsFileData(hdfsURL);
        for (String line : hdfsFileData) {
            String[] rowSplit = line.split("\t");
            String key = rowSplit[0];
            String value = rowSplit[1];

            if (StringUtils.isNotBlank(value) && ! key.equals(outlier)) {

                String[] valueSplit = value.split(";");
                String attributeValue = valueSplit[attributeIndex];
                String[] attValueSplit = attributeValue.split("#");

                dataset.addValue(Double.parseDouble(attValueSplit[0]), attributeName + "-Min", key);
                dataset.addValue(Double.parseDouble(attValueSplit[1]), attributeName + "-Max", key);
                dataset.addValue(Double.parseDouble(attValueSplit[2]), attributeName + "-Avg", key);
            }
        }

//        dataset.addValue(15, "schools", "1970");
//        dataset.addValue(30, "schools", "1980");
//        dataset.addValue(60, "schools", "1990");
//        dataset.addValue(120, "schools", "2000");
//        dataset.addValue(240, "schools", "2010");
//        dataset.addValue(300, "schools", "2014");
        return dataset;
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        setTitle("Trend Analysis");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 831, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 526, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(ReportDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(ReportDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(ReportDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(ReportDialog.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the dialog */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                ReportDialog dialog = new ReportDialog(new javax.swing.JFrame(), true);
                dialog.addWindowListener(new java.awt.event.WindowAdapter() {
                    @Override
                    public void windowClosing(java.awt.event.WindowEvent e) {
                        System.exit(0);
                    }
                });
                dialog.setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
}