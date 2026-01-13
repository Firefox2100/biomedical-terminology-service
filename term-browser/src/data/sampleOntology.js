export const sampleOntology = [
  {
    id: 'ANAT',
    label: 'Anatomy',
    description: 'Structures and regions of the body.',
    children: [
      {
        id: 'ANAT:001',
        label: 'Cardiovascular System',
        description: 'Heart, blood vessels, and circulation.',
        children: [
          {
            id: 'ANAT:001.001',
            label: 'Heart',
            description: 'Four-chambered muscular organ that pumps blood.',
          },
          {
            id: 'ANAT:001.002',
            label: 'Arteries',
            description: 'Blood vessels carrying blood away from the heart.',
          },
        ],
      },
      {
        id: 'ANAT:002',
        label: 'Nervous System',
        description: 'Brain, spinal cord, and peripheral nerves.',
      },
    ],
  },
  {
    id: 'DX',
    label: 'Diagnosis',
    description: 'Clinical findings and conditions.',
    children: [
      {
        id: 'DX:101',
        label: 'Infectious Disease',
        description: 'Diseases caused by pathogenic organisms.',
        hasChildren: true,
      },
      {
        id: 'DX:102',
        label: 'Metabolic Disorder',
        description: 'Conditions impacting metabolic processes.',
      },
    ],
  },
]
